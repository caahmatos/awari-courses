def fatoracao(num):
    res = 1
    for i in range(1, num + 1):
        res *= i
    return res

while True:
    n = int(input('Adicione um número inteiro: '))
    if n == 999:
        print('Encerrando o programa...')
        break
    else:
        calc = fatoracao(n)

        print(f"A fatoração de {n} é: {calc}")
