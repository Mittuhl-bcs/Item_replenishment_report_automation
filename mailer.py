import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders


def send_email(attachment_filename):

    try:
        # credentials for usage
        sender_email = 'mithul.murugaadev@building-controls.com'
        sender_password = '' # specify only if necessary
        receiver_emails = ['mithul.murugaadev@building-controls.com']
        subject = 'Replenishment data - checked reports'
        body = 'A report of discrepancies in the Replenishment data is generated is shared through this mail. Please find the attached CSV file.'


        # Set up the MIME
        message = MIMEMultipart()
        message['From'] = sender_email
        message['To'] = ', '.join(receiver_emails)
        message['Subject'] = subject

        # Attach the body
        message.attach(MIMEText(body, 'plain'))

        # Open the file to be sent
        with open(attachment_filename, 'rb') as attachment:
            # Add file as application/octet-stream
            # Email client can usually download this automatically as attachment
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
        server = smtplib.SMTP('smtp.gmail.com', 587) # for Gmail
        server.starttls()
        server.login(sender_email, sender_password)

        # Send email
        server.sendmail(sender_email, receiver_emails, text)

        # Close the SMTP server
        server.quit()
 
        return True

    except Exception as e:
        raise ValueError(e)


#attachment_filename = 'example.csv'

#send_email(attachment_filename)