The GHGA Connector is a command line client facilitating interaction with the file storage infrastructure of GHGA.
To this end, it provides commands for the up- and download of files that interact with the RESTful APIs exposed by the Upload Controller Service (https://github.com/ghga-de/upload-controller-service) and Download Controller Service (https://github.com/ghga-de/download-controller-service), respectively.

When uploading, the Connector expects an unencrypted file that is subsequently encrypted according to the Crypt4GH standard (https://www.ga4gh.org/news_item/crypt4gh-a-secure-method-for-sharing-human-genetic-data/) and only afterwards uploaded to the GHGA storage infrastructure.

When downloading, the resulting file is still encrypted in this manner and can be decrypted using the Connector's decrypt command.
As the user is expected to download multiple files, this command takes a directory location as input and an optional output directory location can be provided, creating the directory if it does not yet exist (defaulting to the current working directory, if none is provided).

Most of the commands need the submitter's private key that matches the public key announced to GHGA.
The private key is used for file encryption in the upload path and decryption of the work package access and work order tokens during download.
Additionally, the decrypt command needs the private key to decrypt the downloaded file.
