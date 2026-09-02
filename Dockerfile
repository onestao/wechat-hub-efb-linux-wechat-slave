FROM python:3.10-slim

ARG OCI_REVISION=""
ARG OCI_VERSION=""

LABEL org.opencontainers.image.source="https://github.com/onestao/wechat-hub-efb-linux-wechat-slave"
LABEL org.opencontainers.image.revision="${OCI_REVISION}"
LABEL org.opencontainers.image.version="${OCI_VERSION}"

ARG KETTLY_ETM_REPO=https://github.com/kettly1260/efb-telegram-master.git
ARG KETTLY_ETM_REF=36b3382ed784efeba176dba269df47d4df0ef4e7

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONUTF8=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential ffmpeg git libmagic1 \
    && rm -rf /var/lib/apt/lists/*

# The image deliberately installs the selected Kettly fork from source at the
# workspace-locked revision. It never substitutes PyPI efb-telegram-master.
RUN git clone "${KETTLY_ETM_REPO}" /opt/efb-telegram-master \
    && git -C /opt/efb-telegram-master checkout "${KETTLY_ETM_REF}" \
    && python -m pip install --no-cache-dir -e /opt/efb-telegram-master

COPY . /opt/efb-linux-wechat-slave
RUN python -m pip install --no-cache-dir -e /opt/efb-linux-wechat-slave

WORKDIR /opt/efb-linux-wechat-slave
VOLUME ["/root/.ehforwarderbot"]

ENTRYPOINT ["ehforwarderbot"]
CMD ["-p", "default"]
