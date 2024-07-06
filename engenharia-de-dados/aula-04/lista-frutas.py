fruits = ['banana', 'maçã', 'pêra', 'uva', 'laranja']

while True:
    res = input("Digite o nome da fruta ou 999 para sair: ")

    if res == '999':
        print("Encerrando o programa...")
        break

    elif res in fruits:
        print(f"A fruta '{res}' já está na lista!")
        
    else:
        fruits.append(res)
        print(f"A fruta '{res}' foi adicionada à lista!")
