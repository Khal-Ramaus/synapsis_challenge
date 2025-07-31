# Gunakan citra dasar Python
FROM python:3.9-slim-bookworm

# Atur direktori kerja di dalam kontainer
WORKDIR /app

# Tambahkan instalasi Java OpenJDK 17
# Ini penting karena PySpark membutuhkan Java
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get clean

# Set variabel lingkungan JAVA_HOME untuk PySpark
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-amd64
ENV PATH $PATH:$JAVA_HOME/bin

# Salin file requirements.txt ke direktori kerja
COPY requirements.txt .

# Instal dependensi Python
RUN pip install --no-cache-dir -r requirements.txt

# Salin semua file dari folder etl ke direktori kerja kontainer
COPY . .

# Beri izin eksekusi pada skrip shell
RUN chmod +x run_etl_scripts.sh