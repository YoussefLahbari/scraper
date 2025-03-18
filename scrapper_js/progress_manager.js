const fs = require('fs');
const path = require('path');
const logger = require('./logger');

/**
 * Manages saving and loading scraping progress to enable resuming operations
 */
class ProgressManager {
    constructor(options = {}) {
        this.options = {
            progressFile: path.join(__dirname, 'progress.json'),
            backupFile: path.join(__dirname, 'progress.backup.json'),
            saveInterval: 5 * 60 * 1000, // 5 minutes
            ...options
        };

        this.progress = {
            currentState: null,
            currentPage: 0,
            processedCompanies: {},
            lastUpdateTime: new Date().toISOString(),
            completedStates: []
        };

        // Set up auto-save timer
        this.saveTimer = null;
    }

    initAutoSave() {
        if (this.saveTimer) {
            clearInterval(this.saveTimer);
        }
        
        this.saveTimer = setInterval(() => {
            this.saveProgress();
            logger.debug('Auto-saved scraping progress');
        }, this.options.saveInterval);
    }

    stopAutoSave() {
        if (this.saveTimer) {
            clearInterval(this.saveTimer);
            this.saveTimer = null;
        }
    }

    loadProgress() {
        try {
            if (fs.existsSync(this.options.progressFile)) {
                const data = fs.readFileSync(this.options.progressFile, 'utf8');
                this.progress = { ...this.progress, ...JSON.parse(data) };
                logger.info('Loaded progress: %s', JSON.stringify({
                    state: this.progress.currentState,
                    page: this.progress.currentPage,
                    companies: Object.keys(this.progress.processedCompanies).length
                }));
                return true;
            }
        } catch (err) {
            logger.error('Error loading progress file: %s', err.message);
            
            // Try backup file
            try {
                if (fs.existsSync(this.options.backupFile)) {
                    const data = fs.readFileSync(this.options.backupFile, 'utf8');
                    this.progress = { ...this.progress, ...JSON.parse(data) };
                    logger.info('Loaded progress from backup');
                    return true;
                }
            } catch (backupErr) {
                logger.error('Error loading backup progress file: %s', backupErr.message);
            }
        }
        return false;
    }

    saveProgress() {
        // Update timestamp
        this.progress.lastUpdateTime = new Date().toISOString();
        
        // Atomic save pattern - first write to temp, then rename
        const tempFile = `${this.options.progressFile}.tmp`;
        try {
            // Write to temp file
            fs.writeFileSync(tempFile, JSON.stringify(this.progress, null, 2));
            
            // Backup current file if it exists
            if (fs.existsSync(this.options.progressFile)) {
                fs.copyFileSync(this.options.progressFile, this.options.backupFile);
            }
            
            // Replace main file with temp file
            fs.renameSync(tempFile, this.options.progressFile);
            return true;
        } catch (err) {
            logger.error('Failed to save progress: %s', err.message);
            return false;
        }
    }

    updateProgress(state, page, processedCompanies) {
        this.progress.currentState = state;
        this.progress.currentPage = page;
        
        // Update processed companies
        if (processedCompanies) {
            if (Array.isArray(processedCompanies)) {
                // Convert array to object with timestamps
                const now = new Date().toISOString();
                processedCompanies.forEach(id => {
                    this.progress.processedCompanies[id] = now;
                });
            } else if (typeof processedCompanies === 'object') {
                // Merge objects
                this.progress.processedCompanies = {
                    ...this.progress.processedCompanies,
                    ...processedCompanies
                };
            }
        }
    }

    markStateCompleted(state) {
        if (!this.progress.completedStates.includes(state)) {
            this.progress.completedStates.push(state);
            this.saveProgress();
        }
    }

    getStateToProcess(states) {
        // First check if we have a current state in progress
        if (this.progress.currentState && states.includes(this.progress.currentState)) {
            return {
                state: this.progress.currentState,
                page: this.progress.currentPage || 0
            };
        }
        
        // Otherwise find first state that's not completed
        for (const state of states) {
            if (!this.progress.completedStates.includes(state)) {
                return {
                    state: state,
                    page: 0
                };
            }
        }
        
        // All states completed
        return null;
    }

    isCompanyProcessed(companyId) {
        return !!this.progress.processedCompanies[companyId];
    }

    reset() {
        this.progress = {
            currentState: null,
            currentPage: 0,
            processedCompanies: {},
            lastUpdateTime: new Date().toISOString(),
            completedStates: []
        };
        this.saveProgress();
    }
}

module.exports = new ProgressManager();
