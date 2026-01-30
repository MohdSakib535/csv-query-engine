class CSVChatApp {
    constructor() {
        this.schemaData = null;
        this.chartInstances = new Map();
        this.datasetReady = false;
        this.messageCounter = 0;

        this.cacheElements();
        this.bindEvents();
        this.seedChat();
    }

    cacheElements() {
        this.uploadForm = document.getElementById('uploadForm');
        this.fileInput = document.getElementById('csvFile');
        this.uploadBtn = document.getElementById('uploadBtn');
        this.datasetStatus = document.getElementById('datasetStatus');
        this.datasetName = document.getElementById('datasetName');
        this.datasetColumns = document.getElementById('datasetColumns');
        this.columnCountBadge = document.getElementById('columnCountBadge');
        this.columnsList = document.getElementById('columnsList');
        this.chatMessages = document.getElementById('chatMessages');
        this.questionInput = document.getElementById('questionInput');
        this.sendBtn = document.getElementById('sendBtn');
        this.useAI = document.getElementById('useAI');
    }

    bindEvents() {
        this.uploadForm.addEventListener('submit', (event) => this.handleUpload(event));
        this.sendBtn.addEventListener('click', () => this.handleQuery());
        this.questionInput.addEventListener('keydown', (event) => {
            if (event.key === 'Enter' && !event.shiftKey) {
                event.preventDefault();
                this.handleQuery();
            }
        });
        this.fileInput.addEventListener('change', () => {
            const file = this.fileInput.files[0];
            if (file) {
                this.datasetName.textContent = file.name;
            }
        });
    }

    seedChat() {
        this.setDatasetReady(false);
        this.addSystemMessage('Upload a CSV file to start chatting with your data.');
    }

    setDatasetReady(isReady) {
        this.datasetReady = isReady;
        this.questionInput.disabled = !isReady;
        this.sendBtn.disabled = !isReady;

        if (isReady) {
            this.datasetStatus.textContent = 'Ready';
            this.datasetStatus.classList.remove('status-empty');
            this.datasetStatus.classList.add('status-ready');
        } else {
            this.datasetStatus.textContent = 'No dataset';
            this.datasetStatus.classList.remove('status-ready');
            this.datasetStatus.classList.add('status-empty');
            this.datasetName.textContent = 'N/A';
            this.datasetColumns.textContent = '0';
            this.columnCountBadge.textContent = '0';
            this.columnsList.innerHTML = '';
        }
    }

    addSystemMessage(text) {
        const message = document.createElement('div');
        message.className = 'system-message';
        message.textContent = text;
        this.chatMessages.appendChild(message);
        this.scrollChatToBottom();
    }

    addUserMessage(text) {
        const row = document.createElement('div');
        row.className = 'chat-message user';

        const bubble = document.createElement('div');
        bubble.className = 'bubble';
        bubble.textContent = text;

        const group = document.createElement('div');
        group.className = 'user-bubble-group';

        const copyBtn = document.createElement('button');
        copyBtn.className = 'copy-btn';
        copyBtn.type = 'button';
        copyBtn.setAttribute('aria-label', 'Copy message');
        copyBtn.title = 'Copy';
        copyBtn.innerHTML = `
            <span class="icon-copy" aria-hidden="true">
                <svg viewBox="0 0 24 24" fill="none">
                    <rect x="9" y="9" width="10" height="10" rx="2" stroke="currentColor" stroke-width="1.6"></rect>
                    <rect x="5" y="5" width="10" height="10" rx="2" stroke="currentColor" stroke-width="1.6" opacity="0.7"></rect>
                </svg>
            </span>
            <span class="sr-only">Copy</span>
        `;
        copyBtn.addEventListener('click', () => this.copyText(text, copyBtn));

        group.appendChild(bubble);
        group.appendChild(copyBtn);
        row.appendChild(group);
        this.chatMessages.appendChild(row);
        this.scrollChatToBottom();
    }

    addAssistantMessage(contentNode, variant = '') {
        const row = document.createElement('div');
        row.className = 'chat-message assistant';

        const avatar = document.createElement('div');
        avatar.className = 'avatar';
        avatar.textContent = 'AI';

        const bubble = document.createElement('div');
        bubble.className = `assistant-bubble ${variant}`.trim();

        if (typeof contentNode === 'string') {
            bubble.textContent = contentNode;
        } else {
            bubble.appendChild(contentNode);
        }

        row.appendChild(avatar);
        row.appendChild(bubble);
        this.chatMessages.appendChild(row);
        this.scrollChatToBottom();

        return bubble;
    }

    buildLoadingMessage() {
        const wrapper = document.createElement('div');
        wrapper.style.display = 'flex';
        wrapper.style.alignItems = 'center';
        wrapper.style.gap = '12px';

        const dot = document.createElement('div');
        dot.style.width = '10px';
        dot.style.height = '10px';
        dot.style.borderRadius = '50%';
        dot.style.background = 'var(--primary)';
        dot.style.boxShadow = '0 0 0 4px rgba(15, 118, 110, 0.15)';

        const text = document.createElement('span');
        text.textContent = 'Thinking...';

        wrapper.appendChild(dot);
        wrapper.appendChild(text);
        return wrapper;
    }

    async handleUpload(event) {
        event.preventDefault();
        const file = this.fileInput.files[0];

        if (!file) {
            this.addAssistantMessage('Please choose a CSV file before uploading.', 'error');
            return;
        }

        this.uploadBtn.disabled = true;
        this.uploadBtn.textContent = 'Uploading...';

        const formData = new FormData();
        formData.append('file', file);

        try {
            const response = await fetch('/upload', {
                method: 'POST',
                body: formData
            });

            const data = await response.json();

            if (response.ok) {
                this.schemaData = data;
                this.renderColumns(data);
                this.datasetName.textContent = file.name;
                this.datasetColumns.textContent = data.columns.length;
                this.columnCountBadge.textContent = data.columns.length;
                this.setDatasetReady(true);
                this.addSystemMessage(`Dataset ready: ${data.columns.length} columns detected.`);
            } else {
                this.addAssistantMessage(`Upload failed: ${data.detail}`, 'error');
            }
        } catch (error) {
            console.error('Upload error:', error);
            this.addAssistantMessage(`Upload failed: ${error.message}`, 'error');
        } finally {
            this.uploadBtn.disabled = false;
            this.uploadBtn.textContent = 'Upload & Analyze';
        }
    }

    renderColumns(data) {
        this.columnsList.innerHTML = '';
        data.columns.forEach((col) => {
            const chip = document.createElement('div');
            chip.className = 'column-chip';

            const left = document.createElement('div');
            left.innerHTML = `<strong>${col.name}</strong> <span class="column-type">${col.type}</span>`;

            const right = document.createElement('span');
            right.className = 'column-semantic';
            right.textContent = col.semantic_type || 'other';

            chip.appendChild(left);
            chip.appendChild(right);
            this.columnsList.appendChild(chip);
        });
    }

    async handleQuery() {
        const question = this.questionInput.value.trim();
        const useAI = this.useAI.checked;

        if (!this.datasetReady) {
            this.addAssistantMessage('Please upload a CSV before asking questions.', 'error');
            return;
        }

        if (!question) {
            this.addAssistantMessage('Type a question to continue.', 'error');
            return;
        }

        this.addUserMessage(question);
        this.questionInput.value = '';

        const assistantBubble = this.addAssistantMessage(this.buildLoadingMessage());

        try {
            const response = await fetch('/query', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    question,
                    use_ai: useAI
                })
            });

            const data = await response.json();

            assistantBubble.innerHTML = '';

            if (response.ok) {
                assistantBubble.appendChild(this.buildResultCard(data));
            } else {
                const errorDisplay = this.formatErrorDetail(data.detail);
                if (errorDisplay.tone === 'error') {
                    assistantBubble.classList.add('error');
                } else {
                    assistantBubble.classList.remove('error');
                }
                assistantBubble.textContent = errorDisplay.text;
            }
        } catch (error) {
            console.error('Query error:', error);
            const errorDisplay = this.formatErrorDetail(error.message);
            if (errorDisplay.tone === 'error') {
                assistantBubble.classList.add('error');
            }
            assistantBubble.textContent = errorDisplay.text;
        }
    }

    buildResultCard(data) {
        const wrapper = document.createElement('div');

        const columns = data.rows.length > 0 ? Object.keys(data.rows[0]) : [];

        if (data.rows.length === 0) {
            const emptyText = document.createElement('div');
            emptyText.className = 'no-data-text';
            emptyText.textContent = 'No data found.';
            return emptyText;
        }

        const summary = document.createElement('div');
        summary.className = 'result-summary';
        summary.innerHTML = `
            <span class="summary-pill">Rows: ${data.rows.length}</span>
            <span class="summary-pill">Columns: ${columns.length}</span>
            <span class="summary-pill">Mode: ${data.sql ? 'SQL generated' : 'N/A'}</span>
        `;

        const tableContainer = document.createElement('div');
        tableContainer.className = 'result-table';
        tableContainer.appendChild(this.buildTable(data.rows));

        const chartPanel = document.createElement('div');
        chartPanel.className = 'chart-panel';
        const chartGrid = document.createElement('div');
        chartGrid.className = 'chart-grid';
        chartPanel.appendChild(chartGrid);

        const grid = document.createElement('div');
        grid.className = 'result-grid';
        grid.appendChild(tableContainer);
        grid.appendChild(chartPanel);

        const actions = document.createElement('div');
        actions.className = 'result-actions';
        const exportBtn = document.createElement('button');
        exportBtn.className = 'btn btn-primary';
        exportBtn.textContent = 'Download CSV';
        exportBtn.addEventListener('click', () => this.exportToCSV(data.rows));
        actions.appendChild(exportBtn);

        wrapper.appendChild(summary);
        wrapper.appendChild(grid);
        wrapper.appendChild(actions);

        const chartCount = this.renderCharts(data.rows, chartGrid);
        if (chartCount > 1) {
            grid.classList.add('stacked');
        }

        return wrapper;
    }

    formatErrorDetail(detail) {
        if (!detail) {
            return { text: 'Query failed. Please try again.', tone: 'error' };
        }

        const raw = String(detail);
        const message = raw.replace(/^Error executing query:\\s*/i, '').trim();

        if (message.includes('No CSV uploaded')) {
            return { text: 'Please upload a CSV file before asking questions.', tone: 'error' };
        }

        if (message.includes('Question required')) {
            return { text: 'Please enter a question about your data.', tone: 'soft' };
        }

        if (message.includes('Binder Error') && message.includes('strftime')) {
            return { text: 'Date parsing failed. Your date column is likely stored as text. Try reformatting the CSV date column or ask a question without year/month extraction.', tone: 'soft' };
        }

        if (message.toLowerCase().includes("didn't get your query")) {
            return { text: "Sorry, I didn't get your query for this CSV. Try using column names from the left panel.", tone: 'soft' };
        }

        if (message.includes('Binder Error') || message.includes('Catalog Error') || message.includes('Referenced column') || message.includes('No function matches')) {
            return { text: "Sorry, I didn't get that query for this CSV. Try rephrasing or use available column names.", tone: 'soft' };
        }

        if (message.toLowerCase().includes('no results')) {
            return { text: 'No data matched the question. Try a broader query.', tone: 'soft' };
        }

        return { text: message, tone: 'error' };
    }

    buildTable(rows) {
        const table = document.createElement('table');
        if (!rows.length) {
            const tbody = document.createElement('tbody');
            const tr = document.createElement('tr');
            const td = document.createElement('td');
            td.textContent = 'No results to display.';
            td.style.padding = '20px';
            tr.appendChild(td);
            tbody.appendChild(tr);
            table.appendChild(tbody);
            return table;
        }

        const thead = document.createElement('thead');
        const headerRow = document.createElement('tr');
        Object.keys(rows[0]).forEach((key) => {
            const th = document.createElement('th');
            th.textContent = key;
            headerRow.appendChild(th);
        });
        thead.appendChild(headerRow);
        table.appendChild(thead);

        const tbody = document.createElement('tbody');
        rows.forEach((row) => {
            const tr = document.createElement('tr');
            Object.values(row).forEach((value) => {
                const td = document.createElement('td');
                td.textContent = value !== null ? value : 'NULL';
                tr.appendChild(td);
            });
            tbody.appendChild(tr);
        });
        table.appendChild(tbody);
        return table;
    }

    renderCharts(rows, container) {
        container.innerHTML = '';

        if (!rows.length) {
            const empty = document.createElement('div');
            empty.className = 'chart-empty';
            empty.textContent = 'No data to visualize.';
            container.appendChild(empty);
            return 0;
        }

        const columns = Object.keys(rows[0]);
        const numericColumns = columns.filter((col) => this.isNumericColumn(rows, col));
        const dateColumns = columns.filter((col) => this.isDateColumn(col, rows));

        // Check if data is already aggregated (has count column or few rows with numeric values)
        const isAggregated = columns.includes('count') ||
                           (rows.length <= 20 && numericColumns.length > 0) ||
                           this.looksLikeAggregatedData(rows, numericColumns);

        const labelColumn = dateColumns[0] || columns.find((col) => !numericColumns.includes(col)) || columns[0];

        if (numericColumns.length === 0) {
            const card = this.createChartCard(`Distribution of ${labelColumn}`);
            container.appendChild(card.wrapper);
            this.renderDistributionChart(rows, labelColumn, card.canvas);
            return 1;
        }

        const chartColumns = numericColumns.filter((col) => col !== labelColumn);
        const columnsToRender = chartColumns.length ? chartColumns : [numericColumns[0]];

        columnsToRender.forEach((col) => {
            const title = isAggregated ? `${col} by ${labelColumn}` : `Total ${col} by ${labelColumn}`;
            const card = this.createChartCard(title);
            container.appendChild(card.wrapper);
            if (dateColumns.length) {
                this.renderLineChart(rows, labelColumn, col, card.canvas, isAggregated);
            } else {
                this.renderBarChart(rows, labelColumn, col, card.canvas, isAggregated);
            }
        });

        return columnsToRender.length;
    }

    looksLikeAggregatedData(rows, numericColumns) {
        if (!numericColumns.length) return false;

        // Check if numeric values are mostly integers and relatively small
        let integerCount = 0;
        let totalCount = 0;

        rows.forEach(row => {
            numericColumns.forEach(col => {
                const value = row[col];
                if (value !== null && value !== undefined && value !== '') {
                    totalCount++;
                    if (Number.isInteger(Number(value))) {
                        integerCount++;
                    }
                }
            });
        });

        // If most values are integers, likely aggregated data
        return totalCount > 0 && (integerCount / totalCount) > 0.8;
    }

    createChartCard(title) {
        const wrapper = document.createElement('div');
        wrapper.className = 'chart-card';

        const heading = document.createElement('div');
        heading.className = 'chart-title';
        heading.textContent = title;

        const canvas = document.createElement('canvas');
        canvas.id = `chart-${Date.now()}-${this.messageCounter++}`;

        wrapper.appendChild(heading);
        wrapper.appendChild(canvas);

        return { wrapper, canvas };
    }

    isNumericColumn(rows, column) {
        const sample = rows.slice(0, 20);
        let numericCount = 0;
        sample.forEach((row) => {
            const value = row[column];
            if (value === null || value === undefined || value === '') {
                return;
            }
            if (!isNaN(parseFloat(value)) && isFinite(value)) {
                numericCount += 1;
            }
        });
        return numericCount >= Math.max(1, Math.floor(sample.length * 0.6));
    }

    isDateColumn(column, rows) {
        const name = column.toLowerCase();
        if (name.includes('date') || name.includes('time')) {
            return true;
        }
        const sample = rows.slice(0, 10);
        let parsed = 0;
        sample.forEach((row) => {
            const value = row[column];
            if (!value) {
                return;
            }
            const date = new Date(value);
            if (!Number.isNaN(date.getTime())) {
                parsed += 1;
            }
        });
        return parsed >= Math.max(1, Math.floor(sample.length * 0.6));
    }

    renderLineChart(rows, labelColumn, valueColumn, canvas, isAggregated = false) {
        const ctx = canvas.getContext('2d');

        let labels, values;
        if (isAggregated) {
            // Use data directly for already aggregated results
            labels = rows.map(row => row[labelColumn] ?? 'Unknown');
            values = rows.map(row => Number(row[valueColumn]) || 0);
        } else {
            // Aggregate data for raw results
            const aggregated = this.aggregateByLabel(rows, labelColumn, valueColumn);
            labels = Object.keys(aggregated);
            values = Object.values(aggregated);
        }

        const chart = new Chart(ctx, {
            type: 'line',
            data: {
                labels,
                datasets: [{
                    label: valueColumn,
                    data: values,
                    borderColor: '#0f766e',
                    backgroundColor: 'rgba(15, 118, 110, 0.2)',
                    tension: 0.3,
                    fill: true
                }]
            },
            options: {
                responsive: true,
                plugins: { legend: { display: false } },
                scales: {
                    y: {
                        beginAtZero: true,
                        title: {
                            display: true,
                            text: isAggregated ? 'Value' : 'Sum'
                        },
                        ticks: {
                            callback: function(value) {
                                // Format large numbers
                                if (value >= 1000000) {
                                    return (value / 1000000).toFixed(1) + 'M';
                                } else if (value >= 1000) {
                                    return (value / 1000).toFixed(1) + 'K';
                                }
                                return value;
                            }
                        }
                    }
                }
            }
        });

        this.chartInstances.set(canvas, chart);
    }

    renderBarChart(rows, labelColumn, valueColumn, canvas, isAggregated = false) {
        const ctx = canvas.getContext('2d');

        let labels, values;
        if (isAggregated) {
            // Use data directly for already aggregated results
            labels = rows.map(row => row[labelColumn] ?? 'Unknown');
            values = rows.map(row => Number(row[valueColumn]) || 0);
        } else {
            // Aggregate data for raw results
            const aggregated = this.aggregateByLabel(rows, labelColumn, valueColumn);
            labels = Object.keys(aggregated);
            values = Object.values(aggregated);
        }

        const chart = new Chart(ctx, {
            type: 'bar',
            data: {
                labels,
                datasets: [{
                    label: valueColumn,
                    data: values,
                    backgroundColor: 'rgba(245, 158, 11, 0.7)',
                    borderColor: 'rgba(245, 158, 11, 1)',
                    borderWidth: 1
                }]
            },
            options: {
                responsive: true,
                plugins: { legend: { display: false } },
                scales: {
                    y: {
                        beginAtZero: true,
                        title: {
                            display: true,
                            text: isAggregated ? 'Count/Value' : 'Sum'
                        },
                        ticks: {
                            callback: function(value) {
                                // Format large numbers
                                if (value >= 1000000) {
                                    return (value / 1000000).toFixed(1) + 'M';
                                } else if (value >= 1000) {
                                    return (value / 1000).toFixed(1) + 'K';
                                }
                                return value;
                            }
                        }
                    }
                }
            }
        });

        this.chartInstances.set(canvas, chart);
    }

    renderDistributionChart(rows, labelColumn, canvas) {
        const ctx = canvas.getContext('2d');
        const counts = {};
        rows.forEach((row) => {
            const key = row[labelColumn];
            counts[key] = (counts[key] || 0) + 1;
        });

        const chart = new Chart(ctx, {
            type: 'doughnut',
            data: {
                labels: Object.keys(counts),
                datasets: [{
                    data: Object.values(counts),
                    backgroundColor: ['#0f766e', '#0ea5e9', '#f59e0b', '#f97316', '#84cc16']
                }]
            },
            options: {
                responsive: true,
                plugins: { legend: { position: 'bottom' } }
            }
        });

        this.chartInstances.set(canvas, chart);
    }

    aggregateByLabel(rows, labelColumn, valueColumn) {
        const aggregated = {};
        rows.forEach((row) => {
            const key = row[labelColumn] ?? 'Unknown';
            const value = Number(row[valueColumn]) || 0;
            aggregated[key] = (aggregated[key] || 0) + value;
        });
        return aggregated;
    }

    exportToCSV(rows) {
        if (!rows.length) {
            this.addAssistantMessage('No data to export for this query.', 'error');
            return;
        }

        const headers = Object.keys(rows[0]);
        const csvContent = [
            headers.join(','),
            ...rows.map((row) => headers.map((header) => {
                const value = row[header];
                if (typeof value === 'string' && (value.includes(',') || value.includes('"'))) {
                    return `"${value.replace(/"/g, '""')}"`;
                }
                return value;
            }).join(','))
        ].join('\n');

        const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
        const link = document.createElement('a');
        link.href = URL.createObjectURL(blob);
        link.download = 'query_results.csv';
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
    }

    async copyText(text, button) {
        try {
            if (navigator.clipboard && navigator.clipboard.writeText) {
                await navigator.clipboard.writeText(text);
            } else {
                const textarea = document.createElement('textarea');
                textarea.value = text;
                textarea.style.position = 'fixed';
                textarea.style.opacity = '0';
                document.body.appendChild(textarea);
                textarea.focus();
                textarea.select();
                document.execCommand('copy');
                document.body.removeChild(textarea);
            }
            if (button) {
                button.title = 'Copied';
                button.classList.add('copied');
                setTimeout(() => {
                    button.title = 'Copy';
                    button.classList.remove('copied');
                }, 1500);
            }
        } catch (error) {
            console.error('Copy failed:', error);
        }
    }

    scrollChatToBottom() {
        this.chatMessages.scrollTop = this.chatMessages.scrollHeight;
    }
}

document.addEventListener('DOMContentLoaded', () => {
    new CSVChatApp();
});
