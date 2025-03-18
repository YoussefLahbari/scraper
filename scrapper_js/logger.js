const fs = require('fs');
const path = require('path');
const { format } = require('util');

/**
 * Simple logging utility that writes to both console and file
 */
class Logger {
    constructor(options = {}) {
        // Default options
        this.options = {
            logToFile: true,
            logLevel: 'info',
            logDir: path.join(__dirname, 'logs'),
            filePrefix: 'scraper_',
            ...options
        };

        // Create log directory if it doesn't exist
        if (this.options.logToFile && !fs.existsSync(this.options.logDir)) {
            try {
                fs.mkdirSync(this.options.logDir, { recursive: true });
            } catch (err) {
                console.error('Failed to create log directory:', err);
                this.options.logToFile = false;
            }
        }

        this.logLevels = {
            debug: 0,
            info: 1, 
            warn: 2,
            error: 3,
            critical: 4
        };

        this.currentLogFile = null;
        this.setupLogFile();
    }

    setupLogFile() {
        if (!this.options.logToFile) return;

        const date = new Date();
        const dateStr = `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')}`;
        this.currentLogFile = path.join(
            this.options.logDir, 
            `${this.options.filePrefix}${dateStr}.log`
        );
    }

    shouldLog(level) {
        return this.logLevels[level] >= this.logLevels[this.options.logLevel];
    }

    formatMessage(level, message) {
        const timestamp = new Date().toISOString();
        return `[${timestamp}] [${level.toUpperCase()}] ${message}`;
    }

    writeToFile(formattedMessage) {
        if (!this.options.logToFile) return;

        try {
            fs.appendFileSync(this.currentLogFile, formattedMessage + '\n');
        } catch (err) {
            console.error('Failed to write to log file:', err);
        }
    }

    log(level, message, ...args) {
        if (!this.shouldLog(level)) return;

        let formattedMessage;
        if (args.length > 0) {
            formattedMessage = this.formatMessage(level, format(message, ...args));
        } else {
            formattedMessage = this.formatMessage(level, message);
        }

        // Log to console
        switch (level) {
            case 'debug':
                console.debug(formattedMessage);
                break;
            case 'info':
                console.info(formattedMessage);
                break;
            case 'warn':
                console.warn(formattedMessage);
                break;
            case 'error':
            case 'critical':
                console.error(formattedMessage);
                break;
            default:
                console.log(formattedMessage);
        }

        // Write to file
        this.writeToFile(formattedMessage);
    }

    debug(message, ...args) {
        this.log('debug', message, ...args);
    }

    info(message, ...args) {
        this.log('info', message, ...args);
    }

    warn(message, ...args) {
        this.log('warn', message, ...args);
    }

    error(message, ...args) {
        this.log('error', message, ...args);
    }

    critical(message, ...args) {
        this.log('critical', message, ...args);
    }
}

module.exports = new Logger();
