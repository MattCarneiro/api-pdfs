// Código atualizado com Redis, Garbage Collector e reconexão do RabbitMQ com intervalos de 2, 4, 8, ... segundos

const express = require('express');
const fs = require('fs');
const path = require('path');
const axios = require('axios');
const amqp = require('amqplib/callback_api');
const progress = require('progress-stream');
require('dotenv').config();

// Importação e configuração do Redis
const { createClient } = require('redis');
const redisClient = createClient({
    url: process.env.REDIS_URL,
});

redisClient.on('error', (err) => {
    console.error('Erro ao conectar ao Redis:', err);
});

redisClient.on('connect', () => {
    console.log('Conectado ao Redis');
});

// Inicializa a conexão com o Redis
(async () => {
    try {
        await redisClient.connect();
    } catch (err) {
        console.error('Erro ao conectar ao Redis:', err);
    }
})();

const app = express();
const port = process.env.PORT || 3000;

const API_KEY = process.env.GOOGLE_DRIVE_API_KEY;
const BACKOFF_RETRIES = parseInt(process.env.BACKOFF_RETRIES, 10) || 7; // Usado para operações de fetch
const pdfStoragePath = './public/';
if (!fs.existsSync(pdfStoragePath)) {
    fs.mkdirSync(pdfStoragePath, { recursive: true });
}

app.use(express.static('public'));
app.use(express.json());

// Configurações do RabbitMQ
const RABBITMQ_HOST = process.env.RABBITMQ_HOST;
const RABBITMQ_PORT = process.env.RABBITMQ_PORT;
const RABBITMQ_USER = process.env.RABBITMQ_USER;
const RABBITMQ_PASS = process.env.RABBITMQ_PASS;
const RABBITMQ_VHOST = process.env.RABBITMQ_VHOST;
const QUEUE_TYPE = process.env.RABBITMQ_QUEUE_TYPE;  // lazy ou quorum
let QUEUE_NAME = process.env.QUEUE_NAME;  // Nome base da fila
const PREFETCH_COUNT = parseInt(process.env.PREFETCH_COUNT, 10);
const PROXY_TOKEN = process.env.PROXY_TOKEN;

let channel;
let reconnectAttempts = 1; // Inicia em 1 para que o primeiro delay seja de 2 segundos

// Variável global para armazenar a função callback que inicia o consumidor
let consumerCallback = null;

/**
 * Inicia a conexão com o RabbitMQ, criando a conexão e o canal.
 * Se ocorrer algum erro, agenda nova tentativa com backoff exponencial.
 */
function startRabbitMQConnection(callback) {
    amqp.connect({
        protocol: 'amqp',
        hostname: RABBITMQ_HOST,
        port: RABBITMQ_PORT,
        username: RABBITMQ_USER,
        password: RABBITMQ_PASS,
        vhost: RABBITMQ_VHOST,
    }, function (err, connection) {
        if (err) {
            console.error('[AMQP] Error connecting:', err.message);
            return scheduleReconnect();
        }

        connection.on('error', function (err) {
            if (err.message !== 'Connection closing') {
                console.error('[AMQP] Connection error:', err.message);
            }
            scheduleReconnect();
        });

        connection.on('close', function () {
            console.error('[AMQP] Connection closed, reconnecting...');
            scheduleReconnect();
        });

        connection.createChannel(function (err, ch) {
            if (err) {
                console.error('[AMQP] Error creating channel:', err.message);
                return scheduleReconnect();
            }
            channel = ch;
            channel.on('error', function (err) {
                console.error('[AMQP] Channel error:', err.message);
            });
            channel.on('close', function () {
                console.log('[AMQP] Channel closed');
                scheduleReconnect();
            });
            reconnectAttempts = 1; // Reseta o contador ao conectar com sucesso
            console.log('[AMQP] Conectado e canal criado');
            callback(ch);
        });
    });
}

/**
 * Agenda uma nova tentativa de conexão com delay multiplicado:
 * 2 s, 4 s, 8 s, 16 s, 32 s, ...
 */
function scheduleReconnect() {
    const delay = Math.pow(2, reconnectAttempts) * 1000; // 2, 4, 8, ... segundos
    console.log(`[AMQP] Reconnecting in ${delay} ms (attempt ${reconnectAttempts})`);
    reconnectAttempts++;
    setTimeout(() => {
        startRabbitMQConnection(consumerCallback);
    }, delay);
}

async function fetchWithExponentialBackoff(url, options, retries = BACKOFF_RETRIES) {
    const fetch = (await import('node-fetch')).default;
    let retryCount = 0;
    const maxBackoff = 32000;

    while (retryCount < retries) {
        try {
            const res = await fetch(url, options);
            if (!res.ok) {
                throw new Error(`HTTP error! status: ${res.status}`);
            }
            return res;
        } catch (error) {
            const waitTime = Math.min(Math.pow(2, retryCount) * 1000 + Math.floor(Math.random() * 1000), maxBackoff);
            console.log(`Tentando novamente em ${waitTime} ms...`);
            await new Promise(resolve => setTimeout(resolve, waitTime));
            retryCount++;
        }
    }
    throw new Error(`Falha ao buscar ${url} após ${retries} tentativas`);
}

async function downloadPdf(fileUrl, filePath, index, total) {
    const fetch = (await import('node-fetch')).default;
    const res = await fetchWithExponentialBackoff(fileUrl, {}, BACKOFF_RETRIES);

    const totalSize = res.headers.get('content-length');
    if (totalSize > 100 * 1024 * 1024) { // 100 MB
        console.log(`PDF ${index + 1}/${total} ignorado por ser maior que 100 MB.`);
        return null;
    }

    const str = progress({
        length: totalSize,
        time: 100 // Atualiza a cada 100 ms
    });

    str.on('progress', function (progress) {
        console.log(`Baixando PDF ${index + 1}/${total}: ${Math.round(progress.percentage)}%`);
    });

    const timestamp = new Date().toISOString().replace(/[-:.]/g, "");
    const dest = fs.createWriteStream(filePath.replace('.pdf', `_${timestamp}.pdf`));
    res.body.pipe(str).pipe(dest);

    await new Promise((resolve, reject) => {
        dest.on('finish', resolve);
        dest.on('error', reject);
    });

    const finalPath = dest.path;
    const stats = fs.statSync(finalPath);
    console.log(`PDF ${index + 1}/${total} baixado (${Math.round(((index + 1) / total) * 100)}%)`);
    return finalPath;
}

async function getPdfUrlsFromFolder(folderId) {
    const fetch = (await import('node-fetch')).default;
    const url = `https://www.googleapis.com/drive/v3/files?q='${folderId}'+in+parents+and+mimeType='application/pdf'&key=${API_KEY}&fields=files(id,name,mimeType,size)`;
    const res = await fetchWithExponentialBackoff(url, {}, BACKOFF_RETRIES);
    const data = await res.json();
    if (!data.files || data.files.length === 0) {
        throw new Error('Nenhum PDF encontrado na pasta especificada.');
    }
    return data.files.map(file => ({
        url: `https://drive.google.com/uc?id=${file.id}`,
        name: file.name,
        size: file.size
    }));
}

async function processPdfDownload(msg, attempt = 0, currentIndex = 0, log = '') {
    const { link, Id, context, UserMsg, MsgIdPhoto, MsgIdVideo, MsgIdPdf } = JSON.parse(msg.content.toString());

    try {
        const isFolderLink = link.includes('/folders/');
        const folderIdOrFileId = extractIdFromLink(link);
        let pdfs = [];

        if (isFolderLink) {
            pdfs = await getPdfUrlsFromFolder(folderIdOrFileId);
        } else {
            const pdfUrl = `https://drive.google.com/uc?id=${folderIdOrFileId}`;
            pdfs = [{ url: pdfUrl, name: 'downloaded.pdf', size: null }];
        }

        if (pdfs.length === 0) {
            throw new Error('Nenhum PDF encontrado na pasta ou arquivo especificado.');
        }

        const pdfPaths = [];
        for (let i = currentIndex; i < pdfs.length; i++) {
            const pdfPath = path.join(pdfStoragePath, `${pdfs[i].name}`);
            const finalPath = await downloadPdf(pdfs[i].url, pdfPath, i, pdfs.length);
            if (finalPath) {
                pdfPaths.push(finalPath);
            } else {
                throw new Error(`Erro ao processar o PDF ${i + 1}/${pdfs.length}`);
            }
        }

        const pdfNames = pdfPaths.map(p => path.basename(p));
        console.log(`PDFs baixados e processados: ${pdfNames.join(', ')}`);

        // Agendar para apagar os PDFs após 10 minutos
        setTimeout(() => {
            pdfPaths.forEach(pdfPath => {
                fs.unlink(pdfPath, (err) => {
                    if (err) {
                        console.error(`Erro ao apagar o PDF (${pdfPath}):`, err);
                    } else {
                        console.log(`PDF (${pdfPath}) apagado com sucesso.`);
                    }
                });
            });
        }, 600000); // 600000 ms = 10 minutos

        await axios.post('https://ultra-n8n.neuralbase.com.br/webhook/pdfs', {
            pdfNames,
            Id,
            context,
            UserMsg,
            MsgIdPhoto,
            MsgIdVideo,
            MsgIdPdf,
            link,
            result: true
        }).then(() => {
            console.log('Webhook enviado sem erros');
        }).catch(error => {
            console.error(`Erro ao enviar webhook: ${error}`);
        });

        channel.ack(msg);
    } catch (error) {
        console.error('Erro ao processar o PDF:', error);
        log += `Erro ao processar o PDF: ${error.message}\n    at ${error.stack}\n`;

        if (attempt < BACKOFF_RETRIES) {
            const waitTime = Math.min(Math.pow(2, attempt) * 1000 + Math.floor(Math.random() * 1000), 32000);
            console.log(`Tentando novamente processPdfDownload em ${waitTime} ms... (tentativa ${attempt + 1}/${BACKOFF_RETRIES})`);
            setTimeout(() => processPdfDownload(msg, attempt + 1, currentIndex, log), waitTime);
        } else {
            await axios.post('https://ultra-n8n.neuralbase.com.br/webhook/pdfs', {
                pdfNames: null,
                Id,
                context,
                UserMsg,
                MsgIdPhoto,
                MsgIdVideo,
                MsgIdPdf,
                link,
                result: false,
                reason: log
            }).then(() => {
                console.log('Webhook enviado com erros');
            }).catch(error => {
                console.error(`Erro ao enviar webhook: ${error}`);
            });

            channel.nack(msg, false, false); // Rejeita a mensagem sem reencaminhar
        }
    }
}

function extractIdFromLink(link) {
    const fileIdMatch = link.match(/\/d\/([a-zA-Z0-9-_]+)/);
    const folderIdMatch = link.match(/\/folders\/([a-zA-Z0-9-_]+)/);

    if (fileIdMatch) {
        return fileIdMatch[1];
    } else if (folderIdMatch) {
        return folderIdMatch[1];
    } else {
        return null;
    }
}

// Define a função callback do consumidor para ser reutilizada em reconexões
consumerCallback = function(ch) {
    ch.consume(QUEUE_NAME, async (msg) => {
        try {
            await processPdfDownload(msg);
        } catch (error) {
            console.error('Erro ao processar a mensagem:', error);
            ch.nack(msg, false, false);
        }
    });
};

// Inicia a conexão inicial com o RabbitMQ
startRabbitMQConnection(consumerCallback);

app.post('/download-pdfs', async (req, res) => {
    const { link, Id, context, UserMsg, MsgIdPhoto, MsgIdVideo, MsgIdPdf } = req.body;

    if (!link || !Id) {
        return res.status(400).send('Parâmetros ausentes: link e Id são necessários.');
    }

    // Controle de duplicidade com Redis (janela de 3 minutos)
    const duplicateKey = `pdf_request:${Id}:${link}`;
    try {
        const exists = await redisClient.get(duplicateKey);

        if (exists) {
            console.log(`Solicitação ignorada para Id: ${Id}, Link: ${link} (dentro da janela de 3 minutos)`);
            return res.send({ message: 'Solicitação ignorada (já processada nos últimos 3 minutos).' });
        } else {
            await redisClient.setEx(duplicateKey, 180, 'processed');

            const msg = { link, Id, context, UserMsg, MsgIdPhoto, MsgIdVideo, MsgIdPdf };
            channel.sendToQueue(QUEUE_NAME, Buffer.from(JSON.stringify(msg)), {
                persistent: true
            });

            console.log('Mensagem enviada para a fila');
            res.send({ message: 'Iniciando download dos PDFs.' });
        }
    } catch (error) {
        console.error('Erro ao acessar o Redis:', error);
        res.status(500).send('Erro interno do servidor.');
    }
});

app.get('/download', (req, res) => {
    const { pdfName } = req.query;

    if (!pdfName) {
        return res.status(400).send('Nome do PDF não especificado.');
    }

    const filePath = path.join(pdfStoragePath, pdfName);

    if (!fs.existsSync(filePath)) {
        return res.status(404).send('PDF não encontrado.');
    }

    res.download(filePath, pdfName, (err) => {
        if (err) {
            console.error(`Erro ao baixar o PDF (${pdfName}):`, err);
        }
    });
});

app.listen(port, () => {
    console.log(`Servidor rodando em http://localhost:${port}`);
});

// Força Garbage Collection a cada 60 segundos, se disponível
if (global.gc) {
    setInterval(() => {
        console.log('Forçando Garbage Collection...');
        global.gc();
    }, 60000);
} else {
    console.warn('Garbage collector não disponível. Execute com --expose-gc');
}
