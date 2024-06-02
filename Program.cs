// Вводный курс по Big Data (семинары)
// Урок 1. Введение в Большие Данные
// Сделайте mapper и reducer, чтобы посчитать среднее и дисперсию оценок за фильм.
// Реализация через цикл (предпогаем, что мы не знаем сколько у нас фильмов):

// import json

// n, mean, M2 = 0, 0.0, 0
// for path in Path('imdb-user-reviews').glob('**/*'):
//     if path.is_file() and path.suffix == '.json':
//         with open(path, 'r') as f:
//             info = json.load(f)
//         score = float(info['movieIMDbRating'])
//         n += 1
//         delta = score - mean
//         mean += delta / n
//         M2 += delta * (score - mean)

// print(mean, (M2 / n) ** (1/2))

// На основе этого кода соберите mapper и reducer:

def mapper(path):
    with open(path, 'r') as file:
        data = file.readlines()

    scores = []
    for line in data:
        score = float(line.strip())
        scores.append(score)

    return [(score, score) for score in scores]

def reducer(score_data1, score_data2):
    n1, mean1, m21 = score_data1
    n2, mean2, m22 = score_data2

    n = n1 + n2
    mean = (n1 * mean1 + n2 * mean2) / n
    m2 = m21 + m22 + (n1 * (mean1 - mean) ** 2) + (n2 * (mean2 - mean) ** 2)

    return n, mean, m2