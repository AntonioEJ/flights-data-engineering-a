#!/bin/bash

# ==============================================================================
# Data Download Script for Flights Dataset
# Downloads and extracts flights dataset from AWS S3 public bucket
# ==============================================================================

# Exit on any error
set -e

# Configuration Variables
BUCKET_PATH="s3://itam-analytics-dante/flights-hwk"
FILENAME="flights.zip"
DATA_DIR="data"
FULL_S3_PATH="${BUCKET_PATH}/${FILENAME}"

# ==============================================================================
# Logging Functions
# ==============================================================================

# Function to print timestamped log messages
log_message() {
    local level="$1"
    local message="$2"
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo "[$timestamp] [$level] $message"
}

log_info() {
    log_message "INFO" "$1"
}

log_error() {
    log_message "ERROR" "$1"
}

log_success() {
    log_message "SUCCESS" "$1"
}

# ==============================================================================
# Validation Functions
# ==============================================================================

# Check if AWS CLI is installed
check_aws_cli() {
    log_info "Checking AWS CLI installation..."
    if ! command -v aws &> /dev/null; then
        log_error "AWS CLI is not installed. Please install AWS CLI first."
        log_error "Visit: https://aws.amazon.com/cli/"
        exit 1
    fi
    log_success "AWS CLI is installed"
}

# Check if unzip is installed
check_unzip() {
    log_info "Checking unzip installation..."
    if ! command -v unzip &> /dev/null; then
        log_error "unzip is not installed. Please install unzip first."
        if [[ "$OSTYPE" == "darwin"* ]]; then
            log_error "On macOS: brew install unzip"
        elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
            log_error "On Ubuntu/Debian: sudo apt-get install unzip"
            log_error "On CentOS/RHEL: sudo yum install unzip"
        fi
        exit 1
    fi
    log_success "unzip is installed"
}

# ==============================================================================
# Main Functions
# ==============================================================================

# Create data directory if it doesn't exist
create_data_directory() {
    log_info "Creating data directory if it doesn't exist..."
    if [ ! -d "$DATA_DIR" ]; then
        mkdir -p "$DATA_DIR"
        log_success "Created directory: $DATA_DIR"
    else
        log_info "Directory $DATA_DIR already exists"
    fi
}

# Download dataset from S3
download_dataset() {
    log_info "Checking if dataset file exists..."
    
    if [ -f "$FILENAME" ]; then
        log_info "File $FILENAME already exists. Skipping download."
        return 0
    fi
    
    log_info "Downloading dataset from $FULL_S3_PATH..."
    log_info "This may take several minutes depending on file size and connection speed..."
    
    if aws s3 cp "$FULL_S3_PATH" . --no-sign-request; then
        log_success "Dataset downloaded successfully: $FILENAME"
    else
        log_error "Failed to download dataset from S3"
        log_error "Please check:"
        log_error "  1. Internet connection"
        log_error "  2. S3 bucket path: $FULL_S3_PATH"
        log_error "  3. AWS CLI configuration"
        exit 1
    fi
}

# Extract dataset to data directory
extract_dataset() {
    log_info "Extracting dataset to $DATA_DIR directory..."
    
    # Check if zip file exists
    if [ ! -f "$FILENAME" ]; then
        log_error "Zip file $FILENAME not found. Cannot extract."
        exit 1
    fi
    
    # Extract with overwrite protection
    if unzip -o "$FILENAME" -d "$DATA_DIR/"; then
        log_success "Dataset extracted successfully to $DATA_DIR/"
        
        # Display extracted contents
        log_info "Extracted files:"
        ls -la "$DATA_DIR/"
    else
        log_error "Failed to extract $FILENAME"
        log_error "The zip file might be corrupted or incomplete"
        exit 1
    fi
}

# Cleanup function (optional)
cleanup_zip() {
    log_info "Cleaning up zip file..."
    if [ -f "$FILENAME" ]; then
        rm "$FILENAME"
        log_success "Removed zip file: $FILENAME"
    fi
}

# Display summary information
display_summary() {
    log_info "=== DOWNLOAD SUMMARY ==="
    log_info "Source: $FULL_S3_PATH"
    log_info "Destination: ./$DATA_DIR/"
    
    if [ -d "$DATA_DIR" ]; then
        local file_count=$(find "$DATA_DIR" -type f | wc -l | tr -d ' ')
        local dir_size=$(du -sh "$DATA_DIR" 2>/dev/null | cut -f1 || echo "Unknown")
        log_info "Files extracted: $file_count"
        log_info "Total size: $dir_size"
    fi
    
    log_success "Dataset preparation completed successfully!"
}

# ==============================================================================
# Main Execution
# ==============================================================================

main() {
    log_info "=== STARTING FLIGHTS DATASET DOWNLOAD ==="
    log_info "Script: download_data.sh"
    log_info "Target: $FULL_S3_PATH"
    
    # Pre-flight checks
    check_aws_cli
    check_unzip
    
    # Create directory structure
    create_data_directory
    
    # Download and extract dataset
    download_dataset
    extract_dataset
    
    # Optional: cleanup zip file (uncomment if needed)
    # cleanup_zip
    
    # Display summary
    display_summary
    
    log_success "=== DATASET DOWNLOAD COMPLETED ==="
}

# ==============================================================================
# Error Handling
# ==============================================================================

# Trap errors and provide cleanup
trap 'log_error "Script failed on line $LINENO. Exit code: $?"' ERR

# ==============================================================================
# Script Execution
# ==============================================================================

# Run main function
main "$@"