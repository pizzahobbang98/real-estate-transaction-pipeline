# 로컬 CSV 파일을 S3에 업로드하는 택배기사
import os
import boto3
from dotenv import load_dotenv

# 프로젝트 루트의 .env 파일 로드
load_dotenv(os.path.join(os.path.dirname(__file__), "../../.env"))

AWS_ACCESS_KEY_ID     = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION            = os.getenv("AWS_REGION", "ap-northeast-2")
S3_BUCKET             = os.getenv("S3_BUCKET", "realestate-transaction-pipeline")

# 디버깅용
print("KEY 확인:", AWS_ACCESS_KEY_ID[:5] if AWS_ACCESS_KEY_ID else "None")
# 클라이언트 생성
def get_s3_client():
    return boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )
# 파일 업로드
def upload_to_s3(local_path: str, deal_ymd: str):
    """
    로컬 CSV 파일을 S3에 업로드
    저장 경로: raw/YYYY/MM/output_YYYYMM.csv
    """
    year  = deal_ymd[:4]
    month = deal_ymd[4:]
    s3_key = f"raw/{year}/{month}/output_{deal_ymd}.csv"

    s3 = get_s3_client()
    s3.upload_file(local_path, S3_BUCKET, s3_key)
    print(f"✅ S3 업로드 완료: s3://{S3_BUCKET}/{s3_key}")
    return s3_key

if __name__ == "__main__":
    import os
    print("현재 경로:", os.getcwd())
    print("파일 존재:", os.path.exists("output_history_202603.csv"))
    upload_to_s3("output_history_202603.csv", "202603")