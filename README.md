# Slack Bot

Refer to Screenshots...

# Project Libraries

Pip install Pandas / 
Pip install Gspread / 
Pip install google-api-python-client / 
Pip install python-dotenv / 
Pip install google-auth-httplib2 / 
Pip install slackclient / 
Pip install watchdog / 
Pip install APScheduler /
Pip install Investpy /
# Credentials.json
 
You can Get your key from console.cloud.google.com

Insert This Into Credentials.Json Located Within /SlackBot/External

{
  "type": "service_account",

  "project_id": "your_project-1234567",

  "private_key_id": "Your private key id",

  "private_key": "-----BEGIN PRIVATE KEY-----\n    Insert Your Key Here    PRIVATE KEY-----\n",

  "client_email": "your_service_account_name@your-email-12345.iam.gserviceaccount.com",

  "client_id": "1234567891011",

  "auth_uri": "Insert Here",

  "token_uri": " Insert Here",

  "auth_provider_x509_cert_url": "Insert Here",

  "client_x509_cert_url": " Insert Here",

  "universe_domain": " Insert Here"
}

# .env

Create a slack app first

Insert This Into .env Located In the Main Directory

SLACK_TOKEN = "Your Token"

