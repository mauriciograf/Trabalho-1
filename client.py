import socket
import os

def main():
    # Criação do socket TCP/IP
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # Define o endereço do servidor e a porta
    server_address = ('localhost', 12345)

    # Conecta ao servidor
    client_socket.connect(server_address)

    try:
        # Exibe o menu de opções para o usuário
        print("Escolha uma opção de processamento:")
        print("1 - OpenMP com 2 núcleos")
        print("2 - OpenMP com 4 núcleos")
        print("3 - PThreads com 2 núcleos")
        print("4 - PThreads com 4 núcleos")
        print("5 - Sequencial")

        # Lê a escolha do usuário e envia para o servidor
        choice = input("Digite o número da opção desejada: ")
        client_socket.send(choice.encode())

        # Solicita o nome do arquivo ao usuário
        file_path = input('Digite o caminho do arquivo .csv para enviar (ou "sair" para sair): ')
        
        if file_path.lower() == "sair":
            return

        # Obtém o nome do arquivo a partir do caminho
        file_name = os.path.basename(file_path)

        # Envia o nome do arquivo ao servidor
        client_socket.send(file_name.encode())

        # Envia o conteúdo do arquivo em blocos
        with open(file_path, 'rb') as file:
            while True:
                data = file.read(1024)
                if not data:
                    break
                client_socket.send(data)

        print(f"Arquivo {file_name} enviado com sucesso.")

    finally:
        # Encerra a conexão
        client_socket.close()

if __name__ == "__main__":
    main()
