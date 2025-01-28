
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


def send_email(subject, message):
    try:
        # Email configuration
        sender_email = 'probepoc2023@gmail.com'
        receiver_email = 'probepoc2023@gmail.com'
        password = 'rovqljwppgraopla'  
        # sender_email = 'premkumaransathya@gmail.com'
        # receiver_email = 'premkumaransathya@gmail.com'
        # password = 'xbpu qqzr vyzh cbhj'
        # Email content
        msg = MIMEMultipart()
        msg['From'] = sender_email
        msg['To'] = receiver_email
        msg['Subject'] = f"Manual intervention required for {subject}"
        msg.attach(MIMEText(str(message), 'plain'))
        # Connect to the SMTP server
        with smtplib.SMTP('smtp.gmail.com', 587) as server:
            server.starttls()
            server.login(sender_email, password)
            server.send_message(msg)
    except Exception as e:
        pass
    