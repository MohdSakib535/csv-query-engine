/**
 * CSV Q&A Analytics - Frontend JavaScript
 * Handles all client-side functionality for the CSV analysis application
 */

class CSVAnalyticsApp {
    constructor() {
        this.schemaData = null;
        this.queryResults = null;
        this.currentChart = null;
        this.init();
    }

    init() {
        this.bindEvents();
        this.setupUI();
    }

    bindEvents() {
        // Upload form submission
        document.getElementById('uploadForm').addEventListener('submit', (e) => {
            this.handleUpload(e);
        });

        // Query button click
        document.getElementById('queryBtn').addEventListener('click', () => {
            this.handleQuery();
        });

        // Export CSV button
        document.getElementById('exportBtn').addEventListener('click', () => {
            this.exportToCSV();
        });

        // Enter key support for question input
        document.getElementById('question').addEventListener('keypress', (e) => {
            if (e.key === 'Enter' && !e.shiftKey) {
                e.preventDefault();
                this.handleQuery();
            }
        });
    }

    setupUI() {
        // Add loading overlay template
        this.createLoadingOverlay();
    }

    createLoadingOverlay() {
        const overlay = document.createElement('div');
        overlay.id = 'loadingOverlay';
        overlay.className = 'loading-overlay d-none';
        overlay.innerHTML = `
            <div class="text-center">
                <div class="spinner-border text-primary" role="status">
                    <span class="visually-hidden">Loading...</span>
                </div>
                <div class="mt-2">
                    <strong>Processing your request...</strong>
                </div>
            </div>
        `;
        document.body.appendChild(overlay);
    }

    async handleUpload(event) {
        event.preventDefault();

        const formData = new FormData(event.target);
        const file = formData.get('file');

        if (!file) {
            this.showAlert('Please select a CSV file to upload.', 'warning');
            return;
        }

        this.showLoading('Uploading and analyzing CSV file...');

        try {
            const response = await fetch('/upload', {
                method: 'POST',
                body: formData
            });

            const data = await response.json();

            if (response.ok) {
                this.schemaData = data;
                this.displaySchema(data);
                this.showSection('schemaSection');
                this.showAlert('CSV file uploaded and analyzed successfully!', 'success');
            } else {
                this.showAlert('Error uploading file: ' + data.detail, 'danger');
            }
        } catch (error) {
            console.error('Upload error:', error);
            this.showAlert('Error uploading file: ' + error.message, 'danger');
        } finally {
            this.hideLoading();
        }
    }

    async handleQuery() {
        const question = document.getElementById('question').value.trim();
        const useAI = document.getElementById('useAI').checked;

        if (!question) {
            this.showAlert('Please enter a question about your data.', 'warning');
            return;
        }

        this.setQueryLoading(true);

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

            if (response.ok) {
                this.queryResults = data;
                this.displayResults(data);
                this.showSection('resultsSection');
                this.showAlert('Query executed successfully!', 'success');
            } else {
                this.showAlert('Error running query: ' + data.detail, 'danger');
            }
        } catch (error) {
            console.error('Query error:', error);
            this.showAlert('Error running query: ' + error.message, 'danger');
        } finally {
            this.setQueryLoading(false);
        }
    }

    displaySchema(data) {
        const columnsList = document.getElementById('columnsList');
        columnsList.innerHTML = '';

        // Add summary
        const summaryLi = document.createElement('li');
        summaryLi.className = 'list-group-item bg-light fw-bold';
        summaryLi.innerHTML = `<i class="fas fa-info-circle me-2"></i>Found ${data.columns.length} columns in your CSV`;
        columnsList.appendChild(summaryLi);

        // Add column details
        data.columns.forEach(col => {
            const li = document.createElement('li');
            li.className = 'list-group-item d-flex justify-content-between align-items-center';

            const semantic = col.semantic_type ? ` <small class="text-muted">(${col.semantic_type})</small>` : '';
            li.innerHTML = `
                <div>
                    <strong>${col.name}</strong>
                    <small class="text-muted ms-2">${col.type}</small>
                    ${semantic}
                </div>
                <span class="badge bg-primary">${col.sample_values?.length || 0} samples</span>
            `;
            columnsList.appendChild(li);
        });

        // Update column count in stats
        document.getElementById('schemaColCount').textContent = data.columns.length;
    }

    displayResults(data) {
        // Show results section with animation
        const resultsSection = document.getElementById('resultsSection');
        resultsSection.classList.add('fade-in');

        // Display SQL with syntax highlighting
        this.displaySQL(data.sql);

        // Update summary stats
        this.updateStats(data);

        // Display table
        this.displayTable(data.rows);

        // Display chart
        this.renderChart(data);
    }

    displaySQL(sql) {
        const sqlCode = document.getElementById('sqlCode');

        // Simple SQL syntax highlighting
        const highlightedSQL = sql
            .replace(/\b(SELECT|FROM|WHERE|GROUP BY|ORDER BY|HAVING|LIMIT|JOIN|INNER|LEFT|RIGHT|FULL|ON|AS|AND|OR|NOT|IN|BETWEEN|LIKE|IS|NULL)\b/gi,
                '<span class="text-primary fw-bold">$1</span>')
            .replace(/('[^']*')/g, '<span class="text-success">$1</span>')
            .replace(/(\d+)/g, '<span class="text-info">$1</span>');

        sqlCode.innerHTML = highlightedSQL;
    }

    updateStats(data) {
        document.getElementById('rowCount').textContent = data.rows.length;
        document.getElementById('colCount').textContent = data.rows.length > 0 ? Object.keys(data.rows[0]).length : 0;
        document.getElementById('queryTime').textContent = data.execution_time ? data.execution_time.toFixed(2) + 's' : '--';

        // Add duplicate info if available
        if (data.duplicate_info) {
            document.getElementById('duplicateInfo').textContent = data.duplicate_info;
            document.getElementById('duplicateInfoContainer').style.display = 'block';
        } else {
            document.getElementById('duplicateInfoContainer').style.display = 'none';
        }
    }

    displayTable(rows) {
        const table = document.getElementById('resultsTable');
        table.innerHTML = '';

        if (rows.length === 0) {
            table.innerHTML = `
                <tr>
                    <td class="text-center text-muted py-5">
                        <i class="fas fa-inbox fa-3x mb-3"></i>
                        <br>No results found for your query
                    </td>
                </tr>
            `;
            return;
        }

        // Create header
        const thead = document.createElement('thead');
        thead.className = 'table-dark';
        const headerRow = thead.insertRow();

        Object.keys(rows[0]).forEach(key => {
            const th = document.createElement('th');
            th.textContent = key;
            th.className = 'fw-bold';
            headerRow.appendChild(th);
        });
        table.appendChild(thead);

        // Create body
        const tbody = document.createElement('tbody');
        rows.forEach((row, index) => {
            const tr = tbody.insertRow();
            tr.className = index % 2 === 0 ? 'table-light' : '';

            Object.values(row).forEach(value => {
                const td = tr.insertCell();
                td.textContent = value !== null ? value : 'NULL';
                td.className = 'align-middle';

                // Add special styling for count column
                if (Object.keys(row).includes('count') && td.cellIndex === Object.keys(row).length - 1) {
                    td.className += ' fw-bold text-primary';
                }
            });
        });
        table.appendChild(tbody);
    }

    renderChart(data) {
        const canvas = document.getElementById('chartCanvas');
        const ctx = canvas.getContext('2d');

        // Destroy previous chart
        if (this.currentChart) {
            this.currentChart.destroy();
        }

        if (data.rows.length === 0) {
            ctx.clearRect(0, 0, canvas.width, canvas.height);
            ctx.fillStyle = '#6c757d';
            ctx.font = '16px Arial';
            ctx.textAlign = 'center';
            ctx.fillText('No data to visualize', canvas.width / 2, canvas.height / 2);
            return;
        }

        // Determine chart type based on data
        const columns = Object.keys(data.rows[0]);
        const hasCountColumn = columns.includes('count');
        const numericColumns = columns.filter(col => {
            return data.rows.some(row => !isNaN(parseFloat(row[col])) && isFinite(row[col]));
        });

        if (numericColumns.length >= 2) {
            this.renderLineChart(data, columns, numericColumns);
        } else if (columns.length >= 2) {
            this.renderBarChart(data, columns, hasCountColumn);
        } else {
            this.renderPieChart(data, columns, hasCountColumn);
        }
    }

    renderLineChart(data, columns, numericColumns) {
        const ctx = document.getElementById('chartCanvas').getContext('2d');
        const labels = data.rows.map((_, index) => `Row ${index + 1}`);
        const datasets = numericColumns.slice(0, 3).map((col, index) => ({
            label: col,
            data: data.rows.map(row => parseFloat(row[col]) || 0),
            borderColor: ['#007bff', '#28a745', '#dc3545'][index],
            backgroundColor: ['rgba(0, 123, 255, 0.1)', 'rgba(40, 167, 69, 0.1)', 'rgba(220, 53, 69, 0.1)'][index],
            fill: false,
            tension: 0.1
        }));

        this.currentChart = new Chart(ctx, {
            type: 'line',
            data: { labels, datasets },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { position: 'top' },
                    title: { display: true, text: 'Data Trends' }
                },
                scales: {
                    y: { beginAtZero: true }
                }
            }
        });
    }

    renderBarChart(data, columns, hasCountColumn) {
        const ctx = document.getElementById('chartCanvas').getContext('2d');
        const categoryCol = columns[0];
        const valueCol = hasCountColumn ? 'count' : (columns[1] || columns[0]);

        const aggregatedData = {};
        if (hasCountColumn) {
            // Use count column directly
            data.rows.forEach(row => {
                const key = row[categoryCol];
                const value = parseInt(row.count) || 1;
                aggregatedData[key] = value;
            });
        } else {
            // Manual aggregation
            data.rows.forEach(row => {
                const key = row[categoryCol];
                const value = parseFloat(row[valueCol]) || 1;
                aggregatedData[key] = (aggregatedData[key] || 0) + value;
            });
        }

        this.currentChart = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: Object.keys(aggregatedData),
                datasets: [{
                    label: hasCountColumn ? 'Count' : valueCol,
                    data: Object.values(aggregatedData),
                    backgroundColor: 'rgba(0, 123, 255, 0.8)',
                    borderColor: 'rgba(0, 123, 255, 1)',
                    borderWidth: 1
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { position: 'top' },
                    title: { display: true, text: hasCountColumn ? 'Grouped Data Distribution' : 'Data Distribution' }
                },
                scales: {
                    y: { beginAtZero: true }
                }
            }
        });
    }

    renderPieChart(data, columns, hasCountColumn) {
        const ctx = document.getElementById('chartCanvas').getContext('2d');
        const column = columns[0];
        const aggregatedData = {};

        if (hasCountColumn && columns.length > 1) {
            // Use count column for multi-column data
            data.rows.forEach(row => {
                const key = row[column];
                const value = parseInt(row.count) || 1;
                aggregatedData[key] = value;
            });
        } else {
            // Manual counting for single column
            data.rows.forEach(row => {
                const key = row[column];
                aggregatedData[key] = (aggregatedData[key] || 0) + 1;
            });
        }

        this.currentChart = new Chart(ctx, {
            type: 'pie',
            data: {
                labels: Object.keys(aggregatedData),
                datasets: [{
                    data: Object.values(aggregatedData),
                    backgroundColor: [
                        '#007bff', '#28a745', '#dc3545', '#ffc107', '#6f42c1',
                        '#e83e8c', '#fd7e14', '#20c997', '#6c757d', '#17a2b8'
                    ]
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { position: 'right' },
                    title: { display: true, text: hasCountColumn ? 'Grouped Data Composition' : 'Data Composition' }
                }
            }
        });
    }

    exportToCSV() {
        if (!this.queryResults || !this.queryResults.rows.length) {
            this.showAlert('No data to export. Please run a query first.', 'warning');
            return;
        }

        const headers = Object.keys(this.queryResults.rows[0]);
        const csvContent = [
            headers.join(','),
            ...this.queryResults.rows.map(row =>
                headers.map(header => {
                    const value = row[header];
                    // Escape commas and quotes in CSV
                    if (typeof value === 'string' && (value.includes(',') || value.includes('"'))) {
                        return `"${value.replace(/"/g, '""')}"`;
                    }
                    return value;
                }).join(',')
            )
        ].join('\n');

        const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
        const link = document.createElement('a');
        const url = URL.createObjectURL(blob);
        link.setAttribute('href', url);
        link.setAttribute('download', 'query_results.csv');
        link.style.visibility = 'hidden';
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);

        this.showAlert('CSV file exported successfully!', 'success');
    }

    setQueryLoading(loading) {
        const queryBtn = document.getElementById('queryBtn');
        const querySpinner = document.getElementById('querySpinner');
        const queryBtnText = document.getElementById('queryBtnText');

        queryBtn.disabled = loading;
        if (loading) {
            querySpinner.classList.remove('d-none');
            queryBtnText.textContent = 'Running Query...';
        } else {
            querySpinner.classList.add('d-none');
            queryBtnText.textContent = 'Run Query';
        }
    }

    showLoading(message = 'Loading...') {
        const overlay = document.getElementById('loadingOverlay');
        overlay.querySelector('strong').textContent = message;
        overlay.classList.remove('d-none');
    }

    hideLoading() {
        document.getElementById('loadingOverlay').classList.add('d-none');
    }

    showSection(sectionId) {
        document.getElementById(sectionId).style.display = 'block';
        document.getElementById(sectionId).scrollIntoView({ behavior: 'smooth' });
    }

    showAlert(message, type = 'info') {
        // Remove existing alerts
        const existingAlerts = document.querySelectorAll('.alert');
        existingAlerts.forEach(alert => alert.remove());

        // Create new alert
        const alertElement = document.createElement('div');
        alertElement.className = `alert alert-${type} alert-dismissible fade show position-fixed`;
        alertElement.style.cssText = 'top: 20px; right: 20px; z-index: 9999; min-width: 300px;';
        alertElement.innerHTML = `
            ${message}
            <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
        `;

        document.body.appendChild(alertElement);

        // Auto-dismiss after 5 seconds
        setTimeout(() => {
            if (alertElement.parentElement) {
                alertElement.remove();
            }
        }, 5000);
    }
}

// Initialize the application when DOM is loaded
document.addEventListener('DOMContentLoaded', () => {
    window.csvAnalyticsApp = new CSVAnalyticsApp();
});