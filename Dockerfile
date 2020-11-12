FROM node:14-alpine

ARG dng_user=anonymous
ARG dng_pass=anonymous
ARG dng_server=https://jazz.xyz.com

ARG mms_user=$dng_user
ARG mms_pass=$dng_pass
ARG mms_server=https://mms.openmbee.org

ENV DNG_USER $dng_user
ENV DNG_PASS $dng_pass
ENV DNG_SERVER $dng_server
ENV MMS_USER $mms_user
ENV MMS_PASS $mms_pass
ENV MMS_SERVER $mms_server

COPY . .
RUN npm install
RUN npm link

ENTRYPOINT ["dng-mdk"]
