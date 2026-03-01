# rebuild 2025-11-06
FROM python:3.12-slim

WORKDIR /app

# 安装系统依赖 - 优化版
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# 复制依赖文件
COPY requirements.txt .

# 安装 Python 依赖 - 优化版
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# 复制项目文件
COPY . .

# 创建非 root 用户运行（安全优化）
RUN useradd -m -u 1000 botuser && chown -R botuser:botuser /app
USER botuser

# 暴露端口
EXPOSE 8080

# 健康检查（新增）
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
  CMD python -c "import requests; requests.get('http://localhost:8080/health')" || exit 1

# 启动命令
CMD ["python", "main.py"]
