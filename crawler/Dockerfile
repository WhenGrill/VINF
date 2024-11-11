FROM python:3.11

RUN apt-get update && apt-get install -y \
    fonts-liberation \
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libatspi2.0-0 \
    libcups2 \
    libdbus-1-3 \
    libdrm2 \
    libgbm1 \
    libgtk-3-0 \
    libnspr4 \
    libnss3 \
    libu2f-udev \
    libvulkan1 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxkbcommon0 \
    libxrandr2 \
    xdg-utils

RUN wget -N https://chromedriver.storage.googleapis.com/114.0.5735.90/chromedriver_linux64.zip -P /
RUN wget -N https://chrome-versions.com/google-chrome-stable-114.0.5735.90-1.deb -P /

RUN dpkg -i /google-chrome-stable-114.0.5735.90-1.deb

RUN unzip /chromedriver_linux64.zip -d /

RUN mv -f /chromedriver /usr/local/bin/chromedriver

RUN chmod 0755 /usr/local/bin/chromedriver

RUN rm /chromedriver_linux64.zip
RUN rm /google-chrome-stable-114.0.5735.90-1.deb


WORKDIR /app

COPY ./requirements.txt /app/requirements.txt

WORKDIR /app

RUN pip3 install -r requirements.txt

USER www-data:www-data

