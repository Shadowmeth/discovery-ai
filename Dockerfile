FROM python:3.12

RUN apt-get update && apt-get install -y ffmpeg \
    # build-essential \
    # libjpeg-dev \
    # zlib1g-dev \
    # libtiff5-dev \
    # libfreetype6-dev \
    # liblcms2-dev \
    # libwebp-dev \
    # tcl8.6-dev \
    # tk8.6-dev \
    # python3-tk \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .

RUN pip install -r requirements.txt

COPY . .

ENV PORT=8080

EXPOSE 8080

CMD exec functions-framework --target=$FUNCTION_TARGET --port=8080
