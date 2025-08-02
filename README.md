# Weather Data Pipeline

A Python-based weather data extraction pipeline that fetches current weather data from the WeatherStack API and stores it in AWS S3 in Parquet format.

## Overview

This project provides an automated solution for extracting weather data for New Delhi, India, and storing it in a structured format for further analysis. The pipeline fetches real-time weather information and saves it as compressed Parquet files in an S3 bucket with daily partitioning.

## Features

- **Real-time Weather Data Extraction**: Fetches current weather data from WeatherStack API
- **Cloud Storage**: Automatically uploads data to AWS S3 in compressed Parquet format
- **Data Compression**: Uses Gzip compression for efficient storage
- **Comprehensive Logging**: Detailed logging for monitoring and debugging
- **Error Handling**: Robust error handling for API requests and S3 operations

## Project Structure

```
weather-project/
├── README.md
├── requirements.txt
├── extract/
│   └── extract_weather_data.py # Main extraction script
└── logs/
    └── weather_data_extractor.log # Application logs
```

## Prerequisites

- Python 3.9+
- AWS Account with S3 access
- WeatherStack API account

## Installation

1. **Clone the repository**:

   ```bash
   git clone <repository-url>
   cd weather-project
   ```

2. **Create a virtual environment** (recommended):

   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

## Configuration

1. **Create a `.env` file** in the project root:

   ```env
   WEATHERSTACK_API_KEY=your_weatherstack_api_key
   S3_BUCKET_NAME=your_s3_bucket_name
   ```

2. **Configure AWS credentials** (choose one method):
   - AWS CLI: `aws configure`
   - Environment variables: `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`
   - IAM roles (for EC2 instances)

## Usage

### Running the Weather Data Extraction

Execute the main script to fetch and store weather data:

```bash
python extract/extract_weather_data.py
```

### What the Script Does

1. **Loads Environment Variables**: Reads API key and S3 bucket configuration
2. **Fetches Weather Data**: Makes API request to WeatherStack for New Delhi weather
3. **Processes Data**: Converts JSON response to Pandas DataFrame
4. **Stores in S3**: Saves data as compressed Parquet file with date-based naming

### Output Format

Data is stored in S3 with the following structure:

- **Path**: `s3://your-bucket/weather_data_raw/YYYYMMDD.parquet.gzip`
- **Format**: Compressed Parquet
- **Partitioning**: Daily partitions based on extraction date

## Dependencies

- **boto3**: AWS SDK for Python
- **pandas**: Data manipulation and analysis
- **requests**: HTTP library for API calls
- **python-dotenv**: Environment variable management
- **pyarrow**: Parquet file format support

## API Information

This project uses the [WeatherStack API](https://weatherstack.com/) to fetch current weather data for New Delhi, India. The API provides:

- Current weather conditions
- Temperature, humidity, wind speed
- Weather descriptions and codes
- Location information

## Logging

The application generates detailed logs stored in `logs/weather_data_extractor.log`, including:

- Execution timestamps
- Success/failure notifications
- Error messages with stack traces
- Function-level debugging information

## Error Handling

The script handles various error scenarios:

- **API Errors**: Network issues, invalid API keys, rate limits
- **AWS Errors**: Missing credentials, S3 access issues
- **Data Processing Errors**: Invalid JSON responses, DataFrame conversion issues

## Scheduling

For automated daily execution, consider setting up:

- **Cron Jobs** (Linux/Mac):

  ```bash
  0 6 * * * /path/to/python /path/to/extract_weather_data.py
  ```

- **AWS Lambda**: For serverless execution
- **AWS EventBridge**: For scheduled cloud-based execution

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is open source and available under the [MIT License](LICENSE).

## Support

For issues and questions:

- Check the logs in `logs/weather_data_extractor.log`
- Review the error messages for specific guidance
- Ensure all environment variables are properly configured
- Verify AWS credentials and S3 bucket permissions

## Future Enhancements

- Support for multiple cities
- Historical data extraction
- Data validation and quality checks
- Integration with data visualization tools
- Real-time monitoring and alerting
