from project import fetch_api_data
from s3_upload import upload_to_s3

def main():
    print("Fetching data from API...")
    data = fetch_api_data()
    print("Data fetched!")

    print("Uploading to S3...")
    result = upload_to_s3(data)
    print(result)

main()
