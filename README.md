# Kafka BatchListener example with Retry / DLQ

### example requests

[http endpoints](kafka-dlq.http)

### kafka using docker

`docker-compose up -d`

### to view all topics

```
docker exec my-kafka-broker kafka-topics --bootstrap-server my-kafka-broker:9092 --list
```

### to view messages in dlq

```
docker exec -it my-kafka-broker kafka-console-producer --bootstrap-server my-kafka-broker:9092 --topic order-dlq
docker exec -it my-kafka-broker kafka-console-producer --bootstrap-server broker:9092 --topic order
docker exec -it my-kafka-broker kafka-console-consumer --bootstrap-server my-kafka-broker:9092 --topic order-dlq --from-beginning
```

### docker-compose
to recreate:
```
docker-compose up -d --build --force-recreate
```

### ssl setup



1. kafka
``` https://github.com/cloud-on-prem/kafka-docker-ssl/blob/master/auto/create-certs.sh
cd config/certs

set TLD="localhost"
set PASSWORD="mypass"

openssl req -new -x509 -keyout fake-ca-1.key -out fake-ca-1.crt -days 9999 -subj "/CN=%TLD%/OU=CIA/O=REA/L=Melbourne/S=VIC/C=AU" -passin pass:%PASSWORD% -passout pass:%PASSWORD%

set i="broker"
keytool -genkey -noprompt -alias %i% -dname "CN=%TLD%, OU=CIA, O=REA, L=Melbourne, S=VIC, C=AU" -keystore kafka.%i%.keystore.jks -keyalg RSA -storepass %PASSWORD% -keypass %PASSWORD%
keytool -keystore kafka.%i%.keystore.jks -alias %i% -certreq -file %i%.csr -storepass %PASSWORD% -keypass %PASSWORD%

openssl x509 -req -CA fake-ca-1.crt -CAkey fake-ca-1.key -in %i%.csr -out %i%-ca1-signed.crt -days 9999 -CAcreateserial -passin pass:%PASSWORD%

keytool -keystore kafka.%i%.keystore.jks -alias CARoot -import -file fake-ca-1.crt -storepass %PASSWORD% -keypass %PASSWORD%
keytool -keystore kafka.%i%.keystore.jks -alias %i% -import -file %i%-ca1-signed.crt -storepass %PASSWORD% -keypass %PASSWORD%

keytool -keystore kafka.%i%.truststore.jks -alias CARoot -import -file fake-ca-1.crt -storepass %PASSWORD% -keypass %PASSWORD%
```

2. spring
   copy

> kafka.broker.keystore.jks  
> kafka.broker.truststore.jks

to resources/client-certs for spring app

### testing ssl cert change

- run ssl setup steps again

```
openssl req -new -x509 -keyout fake-ca-2.key -out fake-ca-2.crt -days 9999 -subj "/CN=%TLD%/OU=CIA/O=REA/L=Melbourne/S=VIC/C=AU" -passin pass:%PASSWORD% -passout pass:%PASSWORD%

set i="broker"
keytool -genkey -noprompt -alias %i% -dname "CN=%TLD%, OU=CIA, O=REA, L=Melbourne, S=VIC, C=AU" -keystore kafka.%i%.keystore.jks -keyalg RSA -storepass %PASSWORD% -keypass %PASSWORD%
keytool -keystore kafka.%i%.keystore.jks -alias %i% -certreq -file %i%.csr -storepass %PASSWORD% -keypass %PASSWORD%

openssl x509 -req -CA fake-ca-2.crt -CAkey fake-ca-2.key -in %i%.csr -out %i%-ca2-signed.crt -days 9999 -CAcreateserial -passin pass:%PASSWORD%

keytool -keystore kafka.%i%.keystore.jks -alias CARoot -import -file fake-ca-2.crt -storepass %PASSWORD% -keypass %PASSWORD%
keytool -keystore kafka.%i%.keystore.jks -alias %i% -import -file %i%-ca2-signed.crt -storepass %PASSWORD% -keypass %PASSWORD%

keytool -keystore kafka.%i%.truststore.jks -alias CARoot -import -file fake-ca-2.crt -storepass %PASSWORD% -keypass %PASSWORD%
```

### k8s

```
mci-t
docker build -t kafka-dlq .
kubectl apply -f config/deployment/deployment.yml
kubectl apply -f config/deployment/service.yml
kubectl apply -f config/deployment/configmaps/test-conf.yml
kubectl rollout restart deploy

kubectl get po
kubectl get svc

kubectl delete deployment kafka-dlq
kubectl delete service kafka-dlq
```