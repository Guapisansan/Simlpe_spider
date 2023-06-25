from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import redis
import yaml


def read_config_file(server="local"):
    with open("../config.yaml", "r", encoding="utf-8") as f:
        data = yaml.load(f, Loader=yaml.FullLoader)
        params = data.get(server)
        return params


config_Data = read_config_file()
pool = redis.ConnectionPool(host=config_Data.get("redis").get("host"), db=15, password='')
# 初始化数据库连接:
mysql_config_dict = config_Data.get("mysql")
SQLALCHEMY_DATABASE_URL = f'mysql+pymysql://{mysql_config_dict.get("user")}:{mysql_config_dict.get("password")}@{mysql_config_dict.get("host")}:{mysql_config_dict.get("port")}/mysql_config_dict.get("databases")?charset=utf8mb4&autocommit=true'
engine = create_engine(SQLALCHEMY_DATABASE_URL, pool_recycle=3600, pool_size=0, max_overflow=1000, pool_pre_ping=True)
# 创建DBSession类型:
SessionLocal = sessionmaker(autocommit=False, autoflush=True, bind=engine)
Base = declarative_base()
