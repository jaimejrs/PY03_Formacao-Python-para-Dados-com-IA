estoque = {}

print('Sistema de Estoque. Comandos: adicionar item qtd / remover item qtd / sair')

while True:
    entrada = input('\n> ').lower().split()
    
    if not entrada or entrada[0] == 'sair':
        break

    if len(entrada) < 3 or not entrada[2].isdigit():
        print('Entrada inválida! Use: comando item quantidade.')
        continue

    comando, item, qtd = entrada[0], entrada[1], int(entrada[2])

    if comando == 'adicionar':
        estoque[item] = estoque.get(item, 0) + qtd
        print(f'Estoque atual de {item}: {estoque[item]}')

    elif comando == 'remover':
        if item in estoque and estoque[item] >= qtd:
            estoque[item] -= qtd
            if estoque[item] == 0: del estoque[item]
            print(f'Restam {estoque.get(item, 0)} de {item}.')
        else:
            print('Erro: Item não encontrado ou quantidade insuficiente.')
    else:
        print('Comando desconhecido.')

print('\nRelatório Final:', estoque)