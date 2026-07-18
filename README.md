# grpc-cache

Кэширование ответов unary gRPC-методов через политики, объявленные прямо в `.proto`.
Какие методы кэшировать, на какое время и с привязкой ли к актору — задаётся опцией метода, а не кодом.

## Возможности

- Декларативные политики кэша через расширение `MethodOptions` — ничего не нужно прописывать в бизнес-логике.
- TTL на метод; `ttl = 0` — без истечения.
- Кэш с привязкой к актору (`actor_scoped`) — свой ключ на каждого пользователя.
- Подключаемый бэкенд кэша (по умолчанию — in-memory), любой свой через интерфейс `Cache`.
- Настраиваемое поведение при сбое кэша: пропустить в хендлер или вернуть ошибку.
- Защита от cache stampede: конкурентные промахи с одним ключом схлопываются в один вызов хендлера (singleflight).

## Установка

```bash
go get github.com/murouse/grpc-cache
```

## Быстрый старт

### 1. Объявите политику в `.proto`

```proto
import "google/protobuf/duration.proto";
// импорт файла с расширением policy из этой библиотеки

service UserService {
  rpc GetUser(GetUserRequest) returns (GetUserResponse) {
    option (murouse.grpc_cache.v1.policy) = {
      ttl: { seconds: 60 }
      actor_scoped: true
    };
  }
}
```

Методы без политики через интерцептор проходят как есть — не кэшируются.

### 2. Подключите интерцептор

```go
gc := grpccache.New(
    grpccache.WithActorExtractor(myActorExtractor),
    grpccache.WithCacheFailurePolicy(grpccache.CacheFailurePolicyFallbackToHandler),
)

server := grpc.NewServer(
    grpc.UnaryInterceptor(gc.UnaryServerInterceptor()),
)
```

По умолчанию используется встроенный in-memory кэш — для старта больше ничего не нужно.

## Опции

| Опция                              | Назначение                                    | По умолчанию      |
|------------------------------------|-----------------------------------------------|-------------------|
| `WithNamespace(namespace)`         | Пространство имен                             | `"default"`       |
| `WithVersion(version)`             | Версия кэша                                   | `"v1"`            |
| `WithCache(cache, cacheMissError)` | Свой бэкенд и его sentinel-ошибка промаха     | in-memory         |
| `WithActorExtractor(extractor)`    | Извлечение идентификатора актора из контекста | не задан          |
| `WithCacheFailurePolicy(policy)`   | Поведение при ошибке кэша (не промахе)        | `ReturnError`     |
| `WithCacheKeyFormatter(fn)`        | Своя функция построения ключа                 | sha256 по запросу |
| `WithPolicyExtension(ext)`         | Своё расширение политики                      | встроенное        |
| `WithLogger(logger)`               | `*slog.Logger`                                | без логов         |

## Свой бэкенд кэша

Реализуйте интерфейс и передайте его вместе с ошибкой промаха:

```go
type Cache interface {
    Set(ctx context.Context, key string, value interface{}, expiration time.Duration) (string, error)
    Get(ctx context.Context, key string) (string, error)
}
```

`Get` при отсутствии ключа обязан вернуть ту же ошибку, что передана вторым аргументом:

```go
gc := grpccache.New(
    grpccache.WithCache(myRedisCache, redis.Nil),
)
```

Именно по этой ошибке библиотека отличает промах (идём в хендлер) от реального сбоя кэша (применяется `CacheFailurePolicy`).

## Актор

Для `actor_scoped: true` нужен экстрактор:

```go
type ActorExtractor interface {
    IdentifierFromContext(ctx context.Context) (string, bool)
}
```

Идентификатор актора становится частью ключа, поэтому каждый пользователь получает свою запись в кэше. Если экстрактор не задан, а метод помечен `actor_scoped`, запрос завершится ошибкой.

## Поведение при сбое кэша

- `CacheFailurePolicyFallbackToHandler` — при ошибке кэша запрос уходит в хендлер, как будто кэша нет.
- `CacheFailurePolicyReturnError` — ошибка кэша возвращается клиенту.

Промах кэша обрабатывается всегда: считается ответ через хендлер и записывается в кэш. Ошибки хендлера не кэшируются.

## Формат ключа

По умолчанию:

```
grpc-cache:<namespace>:<version>:/<package.Service>/<Method>:<actor>:<sha256(запрос)>
```

Запрос сериализуется детерминированно и хэшируется. Переопределяется через `WithCacheKeyFormatter`.

## О чём помнить

- Кэшируются только **unary**-методы и только те, у которых объявлена политика.
- Метод **без** `actor_scoped` отдаёт один общий закэшированный ответ всем вызывающим. Не помечайте так методы, чей ответ зависит от пользователя, — иначе данные утекут между акторами.
- Детерминированная сериализация protobuf не гарантирует стабильности между версиями библиотеки: обновление может инвалидировать существующие ключи (это холодный кэш, а не ошибка корректности).