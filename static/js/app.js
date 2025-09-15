// JavaScript for SSIS Migration Web App

document.addEventListener('DOMContentLoaded', function() {
    const fileInput = document.getElementById('fileInput');
    const uploadForm = document.getElementById('uploadForm');
    const uploadBtn = document.getElementById('uploadBtn');
    const progressSection = document.getElementById('progressSection');
    const errorAlert = document.getElementById('errorAlert');
    const successAlert = document.getElementById('successAlert');
    const fileList = document.getElementById('fileList');
    const selectedFiles = document.getElementById('selectedFiles');

    // File input change handler
    fileInput.addEventListener('change', function(e) {
        displaySelectedFiles(e.target.files);
    });

    // Form submit handler
    uploadForm.addEventListener('submit', function(e) {
        e.preventDefault();
        
        const files = fileInput.files;
        if (!files || files.length === 0) {
            showError('Please select at least one file to upload.');
            return;
        }

        // Validate file types
        const allowedExtensions = ['.dtsx', '.conmgr', '.params', '.database', '.dtproj'];
        const invalidFiles = [];
        
        for (let file of files) {
            const extension = '.' + file.name.split('.').pop().toLowerCase();
            if (!allowedExtensions.includes(extension)) {
                invalidFiles.push(file.name);
            }
        }

        if (invalidFiles.length > 0) {
            showError(`Invalid file types: ${invalidFiles.join(', ')}. Only SSIS files are allowed.`);
            return;
        }

        uploadFiles(files);
    });

    function displaySelectedFiles(files) {
        if (!files || files.length === 0) {
            fileList.style.display = 'none';
            return;
        }

        selectedFiles.innerHTML = '';
        fileList.style.display = 'block';

        Array.from(files).forEach(file => {
            const li = document.createElement('li');
            li.className = 'list-group-item d-flex justify-content-between align-items-center';
            
            const extension = '.' + file.name.split('.').pop().toLowerCase();
            const icon = getFileIcon(extension);
            
            li.innerHTML = `
                <div>
                    <i class="${icon} text-primary me-2"></i>
                    <span class="file-name">${file.name}</span>
                </div>
                <span class="badge bg-secondary rounded-pill">${formatFileSize(file.size)}</span>
            `;
            
            selectedFiles.appendChild(li);
        });
    }

    function getFileIcon(extension) {
        const iconMap = {
            '.dtsx': 'fas fa-file-code',
            '.conmgr': 'fas fa-plug',
            '.params': 'fas fa-cog',
            '.database': 'fas fa-database',
            '.dtproj': 'fas fa-project-diagram'
        };
        return iconMap[extension] || 'fas fa-file';
    }

    function formatFileSize(bytes) {
        if (bytes === 0) return '0 Bytes';
        const k = 1024;
        const sizes = ['Bytes', 'KB', 'MB', 'GB'];
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
    }

    function uploadFiles(files) {
        const formData = new FormData();
        
        Array.from(files).forEach(file => {
            formData.append('files', file);
        });

        // Show progress
        hideAlerts();
        progressSection.style.display = 'block';
        uploadBtn.disabled = true;
        uploadBtn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Processing...';

        // Upload files
        fetch('/upload', {
            method: 'POST',
            body: formData
        })
        .then(response => response.json())
        .then(data => {
            progressSection.style.display = 'none';
            uploadBtn.disabled = false;
            uploadBtn.innerHTML = '<i class="fas fa-rocket"></i> Start Migration';

            if (data.error) {
                showError(data.error);
            } else {
                showSuccess(data);
            }
        })
        .catch(error => {
            progressSection.style.display = 'none';
            uploadBtn.disabled = false;
            uploadBtn.innerHTML = '<i class="fas fa-rocket"></i> Start Migration';
            showError('Upload failed: ' + error.message);
        });
    }

    function showError(message) {
        hideAlerts();
        document.getElementById('errorMessage').textContent = message;
        errorAlert.style.display = 'block';
        errorAlert.scrollIntoView({ behavior: 'smooth' });
    }

    function showSuccess(data) {
        hideAlerts();
        const message = `Migration completed successfully! Generated ${data.summary.mappings_generated} mappings from ${data.summary.packages_parsed} packages.`;
        document.getElementById('successMessage').textContent = message;
        document.getElementById('viewResultsLink').href = `/results/${data.session_id}`;
        successAlert.style.display = 'block';
        successAlert.scrollIntoView({ behavior: 'smooth' });
    }

    function hideAlerts() {
        errorAlert.style.display = 'none';
        successAlert.style.display = 'none';
    }

    // Drag and drop functionality
    const uploadArea = document.querySelector('.card-body');
    
    ['dragenter', 'dragover', 'dragleave', 'drop'].forEach(eventName => {
        uploadArea.addEventListener(eventName, preventDefaults, false);
        document.body.addEventListener(eventName, preventDefaults, false);
    });

    ['dragenter', 'dragover'].forEach(eventName => {
        uploadArea.addEventListener(eventName, highlight, false);
    });

    ['dragleave', 'drop'].forEach(eventName => {
        uploadArea.addEventListener(eventName, unhighlight, false);
    });

    uploadArea.addEventListener('drop', handleDrop, false);

    function preventDefaults(e) {
        e.preventDefault();
        e.stopPropagation();
    }

    function highlight(e) {
        uploadArea.classList.add('dragover');
    }

    function unhighlight(e) {
        uploadArea.classList.remove('dragover');
    }

    function handleDrop(e) {
        const dt = e.dataTransfer;
        const files = dt.files;
        
        fileInput.files = files;
        displaySelectedFiles(files);
    }
});

// Additional utility functions for results page
function copyToClipboard(elementId) {
    const element = document.getElementById(elementId);
    const text = element.textContent || element.innerText;
    
    navigator.clipboard.writeText(text).then(() => {
        // Show success feedback
        const button = event.target.closest('button');
        const originalHtml = button.innerHTML;
        button.innerHTML = '<i class="fas fa-check"></i> Copied';
        button.classList.remove('btn-outline-secondary');
        button.classList.add('btn-success');
        
        setTimeout(() => {
            button.innerHTML = originalHtml;
            button.classList.remove('btn-success');
            button.classList.add('btn-outline-secondary');
        }, 2000);
    }).catch(err => {
        console.error('Failed to copy text: ', err);
        alert('Failed to copy to clipboard');
    });
}