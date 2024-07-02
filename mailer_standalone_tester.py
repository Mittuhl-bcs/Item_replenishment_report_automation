import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import json


def send_email(attachment_filename, attachment_display_name):

    # Get the credentials stored:
    with open("D:\\Item_replenishment_report_automation\\Credentials.json", "r+") as crednt:
        data = json.load(crednt)
        password = data["password"]

    try:
        # Credentials for usage
        sender_email = "Bcs.notifications@building-controls.com"  # Outlook email address
        sender_password = password  
        receiver_emails = ["mithul.murugaadev@building-controls.com"]  # List of recipient email addresses
        subject = 'Replenishment data - checked reports'
        body = 'A report of discrepancies in the Replenishment data is generated and shared through this mail. Please find the attached ZIP file.'

        # Set up the MIME
        message = MIMEMultipart()
        message['From'] = sender_email
        message['To'] = ', '.join(receiver_emails)
        message['Subject'] = subject

        # Attach the body
        message.attach(MIMEText(body, 'plain'))

        # Open the ZIP file to be sent
        with open(attachment_filename, 'rb') as attachment:
            # Add file as application/octet-stream
            # Email client can usually download this automatically as attachment
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(attachment.read())

        # Encode file in ASCII characters to send by email
        encoders.encode_base64(part)

        # Add header as key/value pair to attachment part
        part.add_header('Content-Disposition', f'attachment; filename="{attachment_display_name}"')

        # Add attachment to message and convert message to string
        message.attach(part)
        text = message.as_string()

        # Log in to SMTP server (for Outlook)
        server = smtplib.SMTP('smtp.office365.com', 587)
        server.starttls()
        server.login(sender_email, sender_password)

        # Send email
        server.sendmail(sender_email, receiver_emails, text)

        # Close the SMTP server
        server.quit()

        return True

    except Exception as e:
        raise ValueError(f'Failed to send email: {e}')


if __name__ == "__main__":
    attachment_filename = "C:\\Users\\Vserve-User\\Downloads\\supplier_review.zip"
    attachment_display_name = "custom_name.zip"
    send_email(attachment_filename, attachment_display_name)