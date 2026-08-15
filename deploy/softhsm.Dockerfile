FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install --no-install-recommends --yes opensc softhsm2 \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /var/lib/softhsm/tokens \
    && printf 'directories.tokendir = /var/lib/softhsm/tokens\nobjectstore.backend = file\n' > /etc/softhsm2.conf

CMD ["sh", "-ec", "if ! softhsm2-util --show-slots | grep -Fq \"${SOFTHSM_TOKEN_LABEL}\"; then softhsm2-util --init-token --free --label \"${SOFTHSM_TOKEN_LABEL}\" --so-pin \"${SOFTHSM_SO_PIN}\" --pin \"${SOFTHSM_USER_PIN}\"; fi; if ! pkcs11-tool --module /usr/lib/softhsm/libsofthsm2.so --login --pin \"${SOFTHSM_USER_PIN}\" --list-objects --type secrkey | grep -Fq 'label:      abt-root'; then pkcs11-tool --module /usr/lib/softhsm/libsofthsm2.so --login --pin \"${SOFTHSM_USER_PIN}\" --keygen --key-type AES:32 --label abt-root --id 01; fi; exec sleep infinity"]
