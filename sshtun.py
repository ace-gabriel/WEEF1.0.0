from sshtunnel import SSHTunnelForwarder
server=SSHTunnelForwarder(
        ('47.52.19.156', 22),  # Remote server ip and port
        ssh_username="gabriel",
        ssh_password="960330",
        ssh_pkey="/Users/gabrielyin/.ssh/id_rsa",
        remote_bind_address=('127.0.0.1', 9200),
)
server.start()  # start ssh server
print('Server connected via SSH')
local_port = str(server.local_bind_port)
print(local_port)
