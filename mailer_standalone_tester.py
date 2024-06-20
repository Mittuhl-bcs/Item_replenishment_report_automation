import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import json

def send_email(attachment_filename):
    # Load credentials from JSON file
    with open("D:\\Replenishment_auotmation_scripts\\Credentials.json", "r") as crednt:
        data = json.load(crednt)
        sender_email = data["sender_email"]
        sender_password = data["password"]

    try:
        # Recipient list
        receiver_emails = ["mithul.murugaadev@building-controls.com", "mithulm@vservesolution.co"]

        # Email content
        subject = 'Replenishment data - checked reports'
        body = 'A report of discrepancies in the Replenishment data is generated is shared through this mail. Please find the attached CSV file.'

        # Setup the MIME
        message = MIMEMultipart()
        message['From'] = sender_email
        message['To'] = ', '.join(receiver_emails)
        message['Subject'] = subject

        # Attach body
        message.attach(MIMEText(body, 'plain'))

        # Open the file to be sent
        with open(attachment_filename, 'rb') as attachment:
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(attachment.read())

        # Encode file in ASCII characters to send by email
        encoders.encode_base64(part)

        # Add header as key/value pair to attachment part
        part.add_header('Content-Disposition', f'attachment; filename= {attachment_filename}')

        # Add attachment to message and convert message to string
        message.attach(part)
        text = message.as_string()

        # Log in to SMTP server
        smtp_server = 'smtp.office365.com'
        smtp_port = 587
        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()

        # Authenticate with OAuth2
        # Example code for OAuth2 authentication (replace with your actual implementation)
        # server.ehlo()
        # server.login(sender_email, sender_password)

        # Send email
        server.login(sender_email, sender_password)
        server.sendmail(sender_email, receiver_emails, text)

        # Close the SMTP server connection
        server.quit()

        return True

    except Exception as e:
        raise ValueError(f'Failed to send email: {e}')

if __name__ == "__main__":
    attachment_filename = "C:\\Users\\Vserve-User\\Downloads\\supplier_review.xlsx"
    send_email(attachment_filename)