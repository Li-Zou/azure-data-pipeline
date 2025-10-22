# functions/data-extractor/__init__.py
import os
import logging
from datetime import datetime
import uuid
from azure.storage.blob import BlobServiceClient
import psycopg2
from psycopg2 import sql

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main() -> str:
    """
    主函数：测试Blob Storage和PostgreSQL连接与操作
    """
    try:
        # 1. 从环境变量读取配置
        config = get_config_from_env()
        logger.info("成功读取环境变量配置")

        # 2. 操作Blob Storage - 上传测试文件
        blob_result = upload_test_file_to_blob(config['storage_connection_string'])
        logger.info(f"Blob Storage操作成功: {blob_result}")

        # 3. 操作PostgreSQL - 创建表并插入数据
        db_result = test_postgres_connection(config)
        logger.info(f"PostgreSQL操作成功: {db_result}")

        return "Success"

    except Exception as e:
        logger.error(f"操作失败: {str(e)}")
        raise

def get_config_from_env() -> dict:
    """
    从环境变量读取配置信息
    """
    storage_connection_string = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
    postgres_host = os.getenv('POSTGRES_HOST')
    postgres_port = os.getenv('POSTGRES_PORT', '5432')
    postgres_db = os.getenv('POSTGRES_DB')
    postgres_user = os.getenv('POSTGRES_USER')
    postgres_password = os.getenv('POSTGRES_PASSWORD')
    
    # 验证必要的环境变量
    if not storage_connection_string:
        raise ValueError("缺少环境变量: AZURE_STORAGE_CONNECTION_STRING")
    
    postgres_required_vars = {
        'POSTGRES_HOST': postgres_host,
        'POSTGRES_DB': postgres_db,
        'POSTGRES_USER': postgres_user,
        'POSTGRES_PASSWORD': postgres_password
    }
    
    missing_vars = [var for var, value in postgres_required_vars.items() if not value]
    if missing_vars:
        raise ValueError(f"缺少PostgreSQL环境变量: {', '.join(missing_vars)}")

    return {
        'storage_connection_string': storage_connection_string,
        'postgres_host': postgres_host,
        'postgres_port': postgres_port,
        'postgres_db': postgres_db,
        'postgres_user': postgres_user,
        'postgres_password': postgres_password
    }

def upload_test_file_to_blob(connection_string: str) -> str:
    """
    向Blob Storage上传测试文件
    """
    try:
        # 创建Blob Service客户端
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        
        # 使用固定容器名
        container_name = "test-container"
        blob_name = f"test-file-{uuid.uuid4()}.txt"
        
        # 创建容器（如果不存在）
        container_client = blob_service_client.get_container_client(container_name)
        try:
            container_client.create_container()
            logger.info(f"创建容器: {container_name}")
        except Exception:
            logger.info(f"容器已存在: {container_name}")

        # 创建测试内容
        test_content = f"""
测试文件内容
上传时间: {datetime.utcnow().isoformat()}
文件ID: {str(uuid.uuid4())}
这是一个用于测试Azure Blob Storage上传功能的文件。
        """.strip()
        
        # 上传文件
        blob_client = container_client.get_blob_client(blob_name)
        blob_client.upload_blob(test_content, overwrite=True)
        
        return f"文件上传成功: {blob_name}"

    except Exception as e:
        logger.error(f"Blob Storage操作失败: {str(e)}")
        raise

def test_postgres_connection(config: dict) -> str:
    """
    测试PostgreSQL连接并执行基本操作
    """
    connection = None
    try:
        # 连接PostgreSQL
        connection = psycopg2.connect(
            host=config['postgres_host'],
            port=config['postgres_port'],
            database=config['postgres_db'],
            user=config['postgres_user'],
            password=config['postgres_password']
        )
        
        # 创建游标
        cursor = connection.cursor()
        
        # 创建测试表
        create_table_query = """
        CREATE TABLE IF NOT EXISTS test_table (
            id SERIAL PRIMARY KEY,
            test_name VARCHAR(100) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            test_data JSONB
        );
        """
        cursor.execute(create_table_query)
        
        # 插入测试数据
        test_id = str(uuid.uuid4())
        insert_query = """
        INSERT INTO test_table (test_name, test_data)
        VALUES (%s, %s)
        RETURNING id;
        """
        cursor.execute(insert_query, (
            f"测试记录 - {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')}",
            {'file_upload': 'success', 'test_id': test_id, 'timestamp': datetime.utcnow().isoformat()}
        ))
        
        # 获取插入的ID
        inserted_id = cursor.fetchone()[0]
        
        # 提交事务
        connection.commit()
        
        return f"表创建成功，插入记录ID: {inserted_id}"

    except Exception as e:
        if connection:
            connection.rollback()
        logger.error(f"PostgreSQL操作失败: {str(e)}")
        raise
    finally:
        if connection:
            connection.close()

# 用于本地测试
if __name__ == "__main__":
    result = main()
    print(f"执行结果: {result}")
