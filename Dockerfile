FROM apache/airflow:2.2.5

ENV USER_UID=50000
ENV USER_GID=0

COPY --chown=${USER_UID}:${USER_GID} requirements.txt ./

RUN pip install -r requirements.txt