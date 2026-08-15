FROM openbao/openbao:2.6.1

USER root
RUN apk add --no-cache softhsm
COPY deploy/openbao-entrypoint.sh /usr/local/bin/abt-openbao-entrypoint.sh
RUN sed -i 's/\r$//' /usr/local/bin/abt-openbao-entrypoint.sh \
    && chmod 555 /usr/local/bin/abt-openbao-entrypoint.sh
ENTRYPOINT ["/usr/local/bin/abt-openbao-entrypoint.sh"]
