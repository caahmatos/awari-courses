def fibonacci():
    fib = [0, 1]

    while True:
        next = fib[-1] + fib[-2]

        if next > 100:
            break
        
        fib.append(next)

    return fib


print("Sequência de Fibonacci até o número 100:")
print(fibonacci())
