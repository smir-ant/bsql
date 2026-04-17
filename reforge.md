# REFORGE — bsql v1.0 архитектурное видение

**Status:** master. Единственный prescriptive документ репозитория. Всё остальное на верхнем уровне (`README.md`, `LICENSE-*`) — служебное.

**Суперсидит:** устаревшие `CREDO.md`, `SPEC.md`, `FEATURES.md`, `near_plan.md`, предыдущие ревизии этого файла — все удалены в reset-коммите 2026-04-15. Git history сохраняет их для археологии, но **ничто из них не является авторитетным**.

**Масштаб:** перезапуск `bsql` с нуля как v1.0. Не v0.28 миграция, не рефактор существующего. Вся кодовая база удалена. Этот документ — blueprint. Код строится по нему, а не наоборот.

---

## Как читать этот документ

**Prescriptive vs descriptive.** Где написано «будет», «должен», «обязан» — это **контракт на реализацию**. Где написано «может», «возможно», «в будущем» — это **открытый вопрос**, будет решён на соответствующей фазе.

**Порядок чтения.** Для нового Claude или контрибьютора:
- Часть I — **обязательно к прочтению целиком**. Без понимания §3 (Принцип №0) всё остальное ломается.
- Части II-III — архитектурные основы. Пропускать нельзя.
- Части IV-VI — по ролям: работаешь с wire — читай IV; с макросами — V; с API — VI.
- Части VII-VIII — справочник safety / features.
- Части IX-XII — операционные (perf, verification, deps, roadmap).

**Когда документ не прав.** Если реализация показывает что какое-то решение в reforge.md ошибочно, **ИЗМЕНЕНИЕ ПРОЦЕССА**:
1. Открывается дискуссия (issue / commit с обоснованием).
2. Принимается решение — обновить reforge.md или придерживаться плана.
3. Коммит с обновлением reforge.md идёт **до** кода который ему противоречит.
4. Silent divergence = причина откатить.

**Приоритет.** Когда два раздела этого документа конфликтуют — **§3 побеждает всегда**. Всегда.

**Live decision tracker.** Файл `deferred.md` в корне репозитория — **живой реестр** каждого отложенного variant'а, tier-downgrade'а и interim-решения. Когда читаешь секцию `reforge.md` про конкретный инвариант или компонент, **перекрёстно сверяйся с `deferred.md`** на предмет актуального статуса. Если документы расходятся — **`deferred.md` побеждает**: он источник правды в реальном времени, `reforge.md` — план на бумаге. Особенно смотри секцию 7 `deferred.md` («Audit-driven architectural commits») — там жёсткие обязательства которые конкретизируют или пересматривают пункты этого документа.

---

# Часть I — Конституция

## §1. Что такое bsql

Библиотека Rust для работы с SQL (PostgreSQL, SQLite; в перспективе MySQL и др.), построенная на двух нераздельных обещаниях:

1. **Если код компилируется — SQL корректен.** Каждый `bsql::query!(...)` валидируется против реальной схемы базы данных на этапе `cargo build`. Опечатки в именах таблиц/колонок, несовпадения типов, забытая nullability — это ошибки компиляции. Ошибки времени выполнения про SQL в bsql отсутствуют как класс.

2. **Если код компилируется — wire cancellation-safe.** Дроп async-future посреди запроса **не может** оставить connection в dirty state. Не детекцией после факта, не восстановлением через flag — **архитектурно невозможно**, потому что ownership wire-state живёт в task-owned сan-I/O state-machine, отделённой от user-visible future.

Эти два обещания **load-bearing**. Всё остальное в bsql существует чтобы их поддерживать.

**Производительность.** На уровне C-библиотек: паритет с libpq на single-row, +10-20% на multi-row через pipelining, +40-50% на batch INSERT через UNNEST. Пиковый RSS в 3-5× меньше аналогов. Zero-alloc на hot-path после прогрева thread-local кешей.

**Эргономика.** `bsql::query!("SELECT id FROM users WHERE active")` — одна строчка, компилятор делает всю работу. Нет DSL. Нет `.filter().select().join()`. Нет builder-паттернов. Чистый SQL в исходнике рядом с Rust-кодом что его использует.

## §2. Чем bsql не является

Явные non-goals. Вопросы «а может добавим X?» из этого списка **блокируются** ссылкой на §2.

- **Не ORM.** Никакой `#[derive(Entity)]`, никаких «сущностей» со связями, никакого unit-of-work. Ты пишешь SQL, bsql проверяет и выполняет.
- **Не query builder.** Никаких `.filter(x.eq(42)).select()`. SQL — это язык, не AST-строитель.
- **Не DSL.** Не изобретаем синтаксис поверх SQL. Поддерживаем ровно тот SQL, что понимает конкретная база.
- **Не database-agnostic.** Поддерживаются PostgreSQL и SQLite (v1.0). MySQL планируется. MSSQL / Oracle / NoSQL — не наш продукт. Каждый backend — полноценный first-class, не адаптер наименьшего общего знаменателя.
- **Не migration tool.** Миграции — внешний инструмент (dbmate, refinery, sqitch, …). bsql валидирует, что миграция не сломает существующие запросы (см. §68), но миграции не пишет и не применяет.
- **Не runtime SQL constructor.** Динамическое построение SQL из `&str` — **не поддерживается** в основном API. `raw_query` существует как escape hatch для DDL с runtime identifier'ами, но он явно помечен и не является SQL-исполнителем.

## §3. Принцип №0 — Архитектурная невозможность как стандарт

**Это load-bearing принцип. Когда любой другой принцип в этом документе ему противоречит — §3 побеждает.**

Каждый класс ошибок в bsql находится **ровно** в одном из трёх уровней:

### 3.1. IMPOSSIBLE-BY-COMPILE

Компилятор Rust **физически не построит** код, выражающий баг. Механизмы:

- `#![forbid(unsafe_code)]` — UB невозможен.
- `#![forbid(clippy::unwrap_used, expect_used, panic, indexing_slicing, as_conversions, arithmetic_side_effects, mem_forget, todo, unimplemented, unreachable)]` — паники и класс silent-truncation / OOB / overflow запрещены.
- Sealed traits (`pub trait T: private::Sealed`) — external crates не могут сломать инвариант trait'а (tier-3 в стабильном Rust; см. §7.5).
- Typestate (zero-sized phantom markers) — операции валидные только в state A методы на типе `T<A>`; вызов на `T<B>` — compile error.
- State-as-data enum variants (данные inline в state) — компилятор требует move/destructure всех carried-полей на каждом transition.
- Exhaustive match с `#![deny(non_exhaustive_omitted_patterns)]` — пропущенные варианты = build fail.
- Bounded types (`heapless::Vec<T, N>`) — переполнение возвращает Err, не panic; OOM unreachable в hot-path.
- `PhantomData<Cell<()>>` field — `!Sync` by construction.
- RAII + `#[must_use]` — forgotten cleanup ловится компилятором.

### 3.2. STRUCTURALLY UNREACHABLE

Архитектура не предоставляет code path к багу. Механизмы:

- API физически не экспонирует panic-ующий метод (sealed newtype над `heapless::Vec` — `insert`/`resize`/`drain` просто отсутствуют).
- Ownership распределён так что требуемое для бага состояние невозможно построить (sans-I/O wire-state owned background task'ом, user future видит только `oneshot::Receiver`).
- Build-system enforcement: `#![no_std]` + отсутствие `alloc` dep — `Box`/`Vec`/`String` физически недоступны в crate'е.
- Dep surface минимальна — класс багов зависимости отсутствует потому что зависимости нет.
- Const bounds и const_assert — значения вне allowed range отлавливаются при сборке.

### 3.3. EXHAUSTIVELY VERIFIED

Там, где (1) и (2) физически невозможны (parser на arbitrary server bytes, concurrent interleavings async task, логические ошибки в собственном коде), bounded input space exhaustively covered:

- `proptest` с 10⁵+ итерациями в nightly CI.
- `Loom` — exhaustive interleaving check каждой concurrent harness.
- `cargo-fuzz` — continuous corpus-guided fuzzing с nightly CI.
- `cargo-mutants` — mutation testing с минимальным kill-rate ≥ 85%.
- Differential testing — сравнение выхода bsql vs `tokio-postgres` / `libpq` на идентичных inputs.

### 3.4. Четвёртого уровня нет

Формулировки, **запрещённые** как обоснование merge'а:

- «Покрыто тестами, не падает» — это техника отладки, не критерий приёмки. Известные тесты ловят known-unknowns; unknown-unknowns остаются открытыми.
- «По договорённости разработчик не делает X» / «Caller must ensure X» — review contracts эквивалентны отсутствию защиты. Будущий refactor молча нарушит.
- «Тут не должно быть ошибок» — отсутствие защиты, не её наличие.
- «Это маловероятный случай» / «in practice X не случается» — input space bounded; всё в bounds должно быть covered. Если что-то outside bounds — это задокументированное ограничение модуля.
- «Эффект не окупается / bottleneck в другом месте» — §3 не торгуется против performance. Zero-cost и absolute safety — ортогональные цели в Rust.
- «Это сложно / долго» — не аргумент.
- **«Tier-1 runtime»** — несуществующая категория. Tier 1 = IMPOSSIBLE-BY-COMPILE. Runtime check (panic, abort, assert) — это **не** tier 1, каким бы loud'ом ни был failure. Runtime check который abort'ит процесс — это tier 2 (structural: bug surfaces loudly, but code with the bug **compiles**). Называть runtime assertion'ы «tier-1» запрещено: это маскирует реальный tier-2 gap за красивым ярлыком.

### 3.5. Применение

Каждый класс ошибок в репозитории мapping'уется к строке матрицы (§51-§62). Каждый PR, который вводит новую поверхность, указывает в commit-body к какому tier'у каждая ключевая failure-class относится. Tier регрессия (с tier 1 вниз на tier 3, или из tier 3 на «happens not to fail») — блокирующий issue, не «техдолг».

**§3 не цель. §3 — критерий приёмки для каждой строки кода.**

## §4. Сопутствующие принципы

### §4.1. Zero-cost — ко-приоритет, не trade-off

Безопасность и производительность в Rust **ортогональны**. Мы не платим за одно другим:

- Typestate zero-sized; phantom markers — 0 байт.
- Sealed traits — compile-time check.
- `#[forbid]` lints — build-time check.
- Bounded types (`heapless::Vec`) — stack-allocated, 0 malloc.
- Sans-I/O separation — sync protocol инлайнит агрессивнее чем async embedded.
- Proof-time / build-time costs (Loom, proptest) — dev-deps only, в shipped binary отсутствуют.

Если видится trade-off между safety и performance — дизайн неверен, искать альтернативу.

### §4.1a. Hot path — zero allocation

«Zero-cost» — абстрактный слоган. Операционное правило ниже.

**Hot path** = код, выполняемый per-query (`push_command`, `feed_bytes`, response dispatch, row decode). Allocation там — **measured failure**, не побочный расход. Policy:

**Иерархия выбора storage** для новых полей / переменных / return types:

1. `[T; N]` — fixed-size array, стек.
2. `heapless::Vec<T, N>` — bounded, стек.
3. `&'buf [T]` — borrowed slice, 0 allocation.
4. `Box<[T]>` — heap, immutable.
5. `Vec<T>` — **крайний случай**, терминальный user-owned API.

**Иерархия выбора** для strings:

1. `&'static str` — compile-time константа.
2. `&'buf str` — borrow from response buffer.
3. `heapless::String<N>` — bounded, стек.
4. `Arc<str>` — shared immutable multi-owner.
5. `Box<str>` — heap immutable.
6. `String` — **крайний случай**, user-owned API.

**Запрещены в hot path:**
- Любой `Vec::new()` / `Vec::with_capacity()` per query.
- Любой `String::new()` / `format!()` / `to_string()` per query.
- Любой `Box::new()` / `Rc::new()` per query (`Arc::clone` OK — refcount).
- Любой `.collect::<Vec<_>>()` на row stream в hot pipeline.

**Cold path** (error formatting, offline cache serialization, logging, connect URL parsing) — allocation приемлема. Hot/cold разграничение явное.

**Verified:** nightly CI benchmark comparing allocator calls per 10K queries (instrumented через `#[global_allocator]` counting hook или `jemalloc-ctl`). Regression > 2% — block merge.

Practical applications scattered across §83 (zero-copy fetch), §84 (thread-local recycling), §89.1 (detailed policy), §89.2 (lazy decode), §89.5 (LUT вместо computation).

### §4.2. Root cause, never symptom

Каждый баг исправляется на уровне где он не может повториться. Если «fix» — проверка, флаг, Drop-guard, особый случай — fix на **неверном уровне**. Правильный уровень: переопределить ownership/типы так чтобы баг нельзя было выразить.

Rule of thumb: если «fix» может быть молча нарушен будущим рефактором, который «забыл проверить флаг» — это не root cause.

### §4.3. Architecture over discipline

Каждое правило, опирающееся на «разработчик помнит X» — review contract. Review contracts запрещены как primary defence. Замена:

- Enforcement через type system.
- Compile-time lints.
- Sealed traits.
- Linear-style type wrappers (`#[must_use]`, panic-on-drop ghosts).
- Build-time verification (Loom, proptest).

Дисциплина — backup для случаев где архитектура физически не достаёт (sealed trait в Rust stable, §7.5). Не primary.

### §4.4. Dependencies are liabilities

Каждый крейт в `Cargo.toml` — attack surface, compile-time cost, version conflict, maintenance burden.

- Core runtime crate (например `bsql-pg-proto`): **1 runtime dep** как baseline.
- Каждая dep — `default-features = false`.
- Перед добавлением dep: можно ли написать 50 строк вместо 50K LoC dep? Когда был последний commit? Сколько transitive deps?
- Периодический аудит. Если можно убрать — убирается.

### §4.5. Long-term > short-term

Между быстрым патчем и архитектурной инвестицией — архитектурная. Project-time 3-5 months для v1.0 rebuild принимается если результат uncompromisable.

«Сейчас некогда, сделаем потом чище» — не принимается. «Потом» не наступит.

### §4.6. Real problems, not manufactured ones

Перед добавлением любого механизма — два вопроса:

1. **Это реально происходило** в production (bsql, похожие библиотеки)? Не «теоретически возможно», а «встретилось у realistic пользователя».
2. **Это может произойти** в обозримых условиях эксплуатации bsql?

Если на оба — «нет», это **manufactured problem**. Проектирование вокруг manufactured problems — over-engineering без real benefit. Так же плохо как under-engineering.

Примеры manufactured что мы не делаем:
- Dual implementation для сверки с собственной же логикой (когда логика одна).
- Абстрактные trait'ы «на будущее» когда импл один.
- Фичи «вдруг кому-то понадобится».

Пример настоящей проблемы: cancellation-leak (#78 в v0.27 servicedesk prod). Реальный bug → архитектурное решение (sans-I/O).

### §4.7. Inline is king

SQL-запрос живёт там где используется. В функции которая вызывает. Не в `.sql`-файле в отдельной директории. Не в `queries/` отдельно. Не в generated bindings. Прямо в Rust-коде, рядом с business logic.

- `bsql::query! { SELECT ... }` в handler-функции. SQL виден без file-hopping.
- IDE: rust-analyzer expands макрос, автокомплит полей, показывает типы.
- Code review: reviewer видит SQL и Rust что его использует в одном diff-hunk.

### §4.8. No blind spots

Каждая nullable колонка — `Option<T>`. Каждый параметр type-verified против `pg_catalog`. Каждая колонка в `SELECT *` резолвится к concrete типу. Каждый cast проверен. Каждая function return type looked-up. No silent failures. No implicit conversions. No «probably works».

**При сомнении → `Option<T>`.** A spurious `.unwrap()` at call site дешёвый to fix when it fires; runtime crash от «surely NOT NULL» assumption дорогой. Validator leans то же: если nullability не доказывается (outer join, subquery, aggregate), колонка — `Option<T>`. Лучше лишний `.unwrap()` в user-коде, чем silent decode panic.

### §4.9. Fail fast, never wait and hope

Timeouts — «я не знаю сколько это займёт, поэтому обрезаю» — admission of helplessness. bsql не wait'ит and hope'ит.

- Pool exhausted → **immediate `PoolExhausted`** по дефолту. Configurable `acquire_timeout` (e.g. 50ms tolerance) — caller's choice.
- Transaction dropped без commit/rollback → connection discarded, warning logged. Next pool user — clean connection.
- Ошибка протокола → immediate classified error, не retry.

Единственные легитимные timeout'ы:
- TCP connect to PG (сеть может быть down; TCP сам ждёт бесконечно).
- PG `statement_timeout` — server-side, PG enforces.

Внутри собственного кода: каждая операция либо **succeeds**, либо **fails immediately**, либо **bounded by resource-it-controls**.

### §4.10. Total query knowledge is a superpower

bsql видит **каждый** `query!` приложения — в compile-time (validation) и в runtime (execution). Это near-complete visibility включает:

- Singleflight coalescing.
- Read/write splitting.
- Statement cache optimization.
- Pipeline batching.
- Migration safety check.
- N+1 detection.
- Compile-time EXPLAIN.

Чем больше queries идёт через `query!`, тем больше bsql может оптимизировать. Поэтому `raw_query` — escape hatch, а не альтернативный путь.

### §4.11. Test discipline (применение §3 к тестам)

Тесты существуют только если покрывают:

1. **Functional spec-conformance.** Делает ли система то что обещает в API-контракте. (Компилятор гарантирует absence-of-error-classes, но не correctness-of-spec.)
2. **Tier 3 verification.** Там где tier 1/2 физически невозможны (parser на random bytes, concurrent interleavings).
3. **Compile-time invariant documentation.** `assert_send<T>()` — генерик-функция которая ломает сборку при регрессии инварианта.

Тесты **НЕ** должны существовать когда покрывают:

1. **Tier 1 invariant.** Компилятор уже гарантирует (тест на `Copy`-semantics, тест на bounded type не превышает cap — типы это гарантируют).
2. **Tier 2 invariant.** Метод физически отсутствует в API (тест что нельзя вызвать приватный метод).
3. **Дубликаты.** Каждый класс поведения тестируется **ровно один раз**.
4. **Implementation details over spec.** Pin ORDER когда wrapper не зависит от order. Pin exact strings когда контракт semantic.

Правило ревью: **каждый новый тест мapping'уется к одной из трёх «должны» категорий** в commit-body или первой строке комментария над тестом. Если категории нет — спрашиваем: «какой архитектурный механизм НЕ гарантирует это свойство?». Ответ «tier 1/2 уже гарантирует» → тест удаляется. Ответ «архитектура слабая» → переписать архитектуру до tier 1/2.

Когда архитектурное изменение поднимает класс с tier 3 на tier 1/2 — тесты покрывавшие этот класс **удаляются немедленно** в том же коммите. «Оставить на всякий случай» запрещено.

#### §4.11.1. Цель — максимум безопасности, не минимум тестов

Tier-1 щит — не монолит. У него есть **узкие швы** где компилятор пропустит well-formed но семантически сломанный код. Тесты закрывают эти швы. **Тесты не минимизируются; они удаляются только когда проверено что конкретная регрессия компилятором поймается.**

Швы, которые тесты обязаны закрывать (и за которыми паника/DoS/уязвимость может проскочить под предлогом «tier-1»):

1. **Литералы в арифметике.** `saturating_add(1)` → `saturating_add(2)` компилируется. const-assert на *другую* пару констант не ловит этот сдвиг.
2. **Swap возвращаемых вариантов в match-руке.** `[] => HeaderParse::Empty` → `[] => HeaderParse::Incomplete` компилируется. Compiler форсит покрытие паттерна, не конкретное return value.
3. **Semantic drift в one-line impl.** `fn eq(&self, other) -> bool { self.value == other.value }` → `self.value == other.value && self.delivered == other.delivered` компилируется. Одна строка, легко изменить.
4. **Arm-body accesses за границами slice pattern.** `[tag, l0, l1, l2, l3, ..]` НЕ запрещает телу arm вызвать `unread.get(5)` — оригинальный `unread` slice остаётся в scope. Pattern пинит *binding*, не *usage*.
5. **Границы классификации.** `if declared < 4` — число `4` литерал. `if declared >= MAX_FRAME_LEN_FIELD` — имя константы компилятор-закреплённое, но значение константы (4095) — литерал.

**Алгоритм решения «удалять или оставить»:**

1. Определи инвариант теста (категория 1/2/3 по §4.11).
2. Замени мысленно исходную строку которую тест наблюдает на well-formed но неверный вариант — swap return, сдвиг литерала, добавление кода в arm body. **Компилируется?**
3. Если **да** — тест несёт tier-1-шов, оставляй.
4. Если **нет** — инвариант действительно архитектурный, тест дубль, можешь удалить.
5. Если **да, но другой тест провалился бы раньше на том же классе регрессии** — можешь удалить, но сначала докажи это указанием на перекрывающий тест в commit-message.

**Блиц-тест антипаттерна**: если аргумент к удалению — «source — one-line identity, компилятор держит» — это **недостаточно**. Нужно проверить вопрос 2 из алгоритма выше. Без этой проверки удаление — **прямой путь к проскоку паники/DoS/leak через щит который выглядел несокрушимым на бумаге**.

Решение должно быть **устойчивым**, не хрупким. Хрупкость — это когда один литерал сдвигает поведение на класс ошибок, компилятор этого не видит, и некому об этом сказать.

---

# Часть II — Архитектура

## §5. Language-level choices

### §5.1. MSRV: 1.95 stable

Фиксируется в `Cargo.toml` через `rust-version`, в CI через `rust-toolchain.toml`.

Latest stable выбран намеренно: CREDO §0 требует использовать **все доступные compile-time механизмы**. Старая MSRV означает отказ от инструментов:

- `#[expect(lint, reason = "…")]` — 1.81+. Lint-exception который **сам фейлит** сборку если lint перестал фиритиь. Критично для tier-1 дисциплины (`#[allow]` запрещён).
- Precise capturing (`impl Trait + use<…>`) — 1.87+. Lifetime bounds в RPITIT становятся tighter.
- `async fn` in traits stabilizations — через 1.75-1.94. Backend trait пишется native async без workaround'ов.
- Const generic expressions — bounded buffers параметризуются чище.
- `let-else`, `if-let chains` — 2024 edition. Control-flow ergonomics.

Обновление MSRV — normal commit в `Cargo.toml` + `rust-toolchain.toml` + CI matrix. Не предмет обсуждения «сломает ли это кого-то»; bsql — высокий бар, не low-common-denominator.

### §5.2. Edition: 2024

Включает:
- Disjoint capture fields в closures (fewer `.clone()`).
- Trailing comma in generics.
- Unsafe expr must be inside `unsafe` block (но у нас `#![forbid(unsafe_code)]`).
- `#[diagnostic::…]` attributes.
- Migration paths из older editions.

### §5.3. Forbid bundle (crate-root lint-set)

Каждый wire-path / core crate начинается с:

```rust
#![forbid(unsafe_code)]
#![forbid(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::todo,
    clippy::unimplemented,
    clippy::unreachable,
    clippy::indexing_slicing,
    clippy::mem_forget,
    clippy::as_conversions,
    clippy::arithmetic_side_effects,
    clippy::float_arithmetic,
    clippy::integer_division,
)]
#![deny(
    unused_must_use,
    unused_lifetimes,
    missing_docs,
    rust_2024_incompatible_pat,
)]
#![warn(
    missing_debug_implementations,
    missing_copy_implementations,
)]
```

Исключения — **только** `#[expect(lint, reason = "…")]` в точке необходимости, с прозой-обоснованием в `reason`. `#[allow(lint)]` без `reason` запрещён полностью (ломает `unused_must_use`-like self-check).

FFI-crate (`bsql-driver-sqlite`) имеет единственное исключение: `ffi.rs` module-level `#![allow(unsafe_code)]` внутри которого **каждый** `unsafe { }` блок сопровождается SAFETY-комментарием с инвариантом. Остальные modules в FFI-crate форбидят unsafe как обычно.

### §5.4. `panic = "abort"`

Workspace release profile использует `panic = "abort"` вместо `unwind`. Последствия:

- Панику никто не catch'ает — процесс падает немедленно.
- Drop'ы не запускаются на панике (но это OK — у нас panic-path нет в дизайне, forbid-bundle их запрещает).
- ~5% smaller binary, чище сгенерированный код.
- Устраняет класс «что если panic во время Drop?» — он unreachable.

Для dev / test profile — `unwind` (чтобы test harness мог отрепортить failing test). Release — abort.

### §5.5. `#![doc = …]`, `#[non_exhaustive]`, `#[must_use]`

- Каждый pub enum который может получить варианты в будущем — `#[non_exhaustive]`. User `match` с catch-all `_ =>` compile'ится; без — compile error при добавлении.
- Каждая pub функция возвращающая `Result` / `Option` / stateful тип — `#[must_use]`. Молчаливый `let result = fn();` warn'ится в user-коде.
- Каждая pub doc'ирована (`#![deny(missing_docs)]`).

### §5.6. Rust 1.95+ features — явный leverage list

Features stable в MSRV 1.95 (или earlier) которые bsql активно использует. Каждый выбран потому что **closes a CREDO §0 gap** или **даёт zero-cost выигрыш**.

| Feature | Stable с | Применение в bsql |
|---|---|---|
| **`#[expect(lint, reason = "…")]`** | 1.81 | Lint-exception что сам **фейлит build** если lint перестал фиритиь. Замена `#[allow]` везде где оно допустимо (ни одного bare `#[allow]` без `reason` в codebase). Критично для самопроверки forbid-bundle дисциплины. |
| **`core::mem::offset_of!`** | 1.77 | Const-eval offset of field в struct. Macro-generated row decoders используют для fixed-offset decode без sizeof-gymnastics. |
| **Precise capturing `impl Trait + use<…>`** | 1.87 | Tight lifetime bounds в RPITIT (`async fn` in traits). Backend's `run_io` signature становится чище + fewer unnecessary lifetime pollution up the call stack. |
| **`async fn` in traits** (stable evolution) | 1.75-1.94 | `Backend` trait пишется native async без `async-trait` dep. Zero Box-dyn-Future overhead. |
| **`core::hint::cold_path()`** | 1.83 | Inline cold-branch hint без extract'а в отдельную `#[cold]` function. Branch prediction уточнение. |
| **`const_refs_to_static`** | 1.83 | `const TABLE: &[u8] = &STATIC_BYTES;` — const references в static tables. LUT'ы из §89.5. |
| **`LazyLock` / `OnceLock`** | 1.80 / 1.70 | Ленивая инициализация static lookup tables (OID→decoder fn-ptr, tag→handler) без `lazy_static!` / `once_cell` deps. Minus one dep. |
| **`#[diagnostic::do_not_recommend]`** | 1.85 | На `raw_query` и `BsqlError::Other` — compiler не suggest'ит их в error messages как «возможно ты хотел это». Guides user toward correct APIs. |
| **Async closures** | 1.85 | `pool.for_each(async \|row\| { await_something(row).await })` — cleaner callback API. Replaces `impl FnMut(Row) -> impl Future<Output = ()>` verbose signature. |
| **`std::hint::black_box`** | 1.66 | Benchmark harness anti-DCE (используется в `bench/`). |
| **Generic `const fn` с richer expressions** | 1.83+ | Wider `const fn` eligibility — позволяет более сложные compile-time вычисления (LUT generators, size calculations). |
| **`if let` chains в stable через 2024 edition** | edition 2024 | `if let Some(x) = a && let Ok(y) = b { ... }` — ergonomic flow в parsers без nested match. |
| **Disjoint capture fields в closures** | 2024 edition | Fewer `.clone()` в closure captures. Zero-cost ergonomics. |
| **`Cell::update`** | 1.80 | Atomic-ish field update в `RefCell<State>` paths. |
| **Edition 2024 patterns** | edition 2024 | `impl Trait` in type aliases в stable forms, extended `Result`/`Option` methods. |
| **`cfg_select!` macro** | 1.95 | Compile-time conditional branching, заменяет `cfg-if` crate. Используется для conditional compilation `#[cfg(test)]` vs `#[cfg(not(test))]` блоков, в частности для DEF-052 (ReplyId Drop-guard diagnostic masking). Zero dep, zero runtime. |

**MSRV bump policy:** поднимается **сразу** когда stable Rust release дает feature, которая closes a tier-1 gap или снимает workaround в codebase. Не consensus-driven, не «wait for ecosystem» — CREDO §0 takes priority over backward compatibility breadth.

## §6. Crate graph

### §6.1. Overview

```
         ┌────────────┐
         │    bsql    │  ← user-facing facade (re-exports)
         │  (фасад)   │
         └──────┬─────┘
                │
       ┌────────┴────────┐
       ▼                 ▼
┌─────────────┐   ┌─────────────┐
│ bsql-macros │   │  bsql-core  │
│ (procedural │   │ (Pool<B>,   │
│   macros)   │   │Transaction, │
│             │   │  errors)    │
└─────────────┘   └──────┬──────┘
                         ▼
                  ┌──────────────┐
                  │ bsql-backend │  ← Backend trait, Client<B>
                  │ (channel arch│     generic over B: Backend
                  │   skeleton)  │
                  └──────┬───────┘
          ┌──────────────┼──────────────┐
          ▼              ▼              ▼
   ┌─────────────┐  ┌──────────┐  ┌──────────┐
   │   PG path   │  │  SQLite  │  │  MySQL   │ ← future
   │  (2 crates) │  │(1 crate) │  │(2 crates)│
   └─────────────┘  └──────────┘  └──────────┘

PG path:
  bsql-pg-proto  (sans-I/O, no_std, pure sync state machine)
       │
       ▼
  bsql-driver-postgres  (async run_io + auth + TLS + codec + PgBackend impl)

SQLite path:
  bsql-driver-sqlite  (FFI + SqliteBackend + spawn_blocking shim)
  (no *-proto split — no wire protocol to abstract)

Shared utilities:
  bsql-arena  (bump allocator for row data; internal helper, не exposed в bsql facade)
```

### §6.2. Per-crate responsibilities

| Crate | Role | No-std? | Tokio? |
|---|---|---|---|
| `bsql` | Facade. Re-exports `query!`, `Pool`, `Transaction`, типы. Zero logic. | No | Yes (reexport) |
| `bsql-macros` | proc-macro crate. `query!`, `pg_enum`, `sort`, `test`, `connect` attribute macros. Online / offline validation pipeline. | N/A (proc-macro) | No |
| `bsql-arena` | Bump allocator с thread-local recycling. Used by row decoders. | Yes, `no_std + alloc` | No |
| `bsql-core` | Generic `Pool<B>`, `Transaction<'pool, B>`, `BsqlError`, `Sensitive<T>`, bounded types, listener. | No | Yes (`sync` + `rt`) |
| `bsql-backend` | `Backend` trait (sealed), `BackendError` supertrait, `Client<B>`, `PingError<B>`. Channel-architecture skeleton. | No | Yes (`sync` + `rt`) |
| `bsql-pg-proto` | Pure sync PostgreSQL wire-protocol state machine. Typestate, state-as-data, bounded buffers. | Yes (`no_std`, no `alloc`) | **No** |
| `bsql-driver-postgres` | `PgBackend` impl. Async `run_io` wrapper. Auth (SCRAM). TLS (rustls). Binary codec. | No | Yes (full) |
| `bsql-driver-sqlite` | `SqliteBackend` impl. FFI wrapper (`ffi.rs` the only unsafe). spawn_blocking async shim. | No | Yes (`rt`) |

### §6.3. Dep graph

```
bsql           ───> bsql-macros, bsql-core, bsql-driver-postgres, bsql-driver-sqlite
bsql-macros    ───> syn, quote, proc-macro2, bitcode, sha2, rapidhash
                    (no bsql-* — macros read bitcode cache, write typed Rust)
bsql-core      ───> bsql-backend, bsql-arena
bsql-backend   ───> tokio (sync + rt)
bsql-pg-proto  ───> heapless
                    (Stage 2 adds: sha2, zeroize, subtle — когда пойдёт SCRAM)
bsql-driver-postgres ───> bsql-pg-proto, bsql-backend, bsql-core, bsql-arena,
                           tokio (full), rustls, ring, webpki-roots, tokio-rustls,
                           rustls-pemfile, rapidhash
bsql-driver-sqlite   ───> bsql-backend, bsql-core, bsql-arena,
                           libsqlite3-sys, crossbeam-channel
bsql-arena     ───> (nothing — pure allocator)
```

**Инвариант:** `bsql-pg-proto` НЕ зависит от `bsql-core` / `bsql-backend`. Проверяется `cargo tree -p bsql-pg-proto` в CI. Sans-I/O sealness — структурная, enforced dep-graph'ом.

**Invariant:** `bsql-macros` НЕ зависит от runtime crates. proc-macro crate видит только типы через `syn` / `quote` / `proc-macro2` и читает bitcode cache. Связь с runtime через **генерируемый код** — он референсит `bsql::…` символы, но сам proc-macro их не линкует.

### §6.4. Workspace Cargo.toml

```toml
[workspace]
resolver = "3"
members = [
    "crates/bsql",
    "crates/bsql-arena",
    "crates/bsql-backend",
    "crates/bsql-core",
    "crates/bsql-driver-postgres",
    "crates/bsql-driver-sqlite",
    "crates/bsql-macros",
    "crates/bsql-pg-proto",
]

[workspace.package]
version = "1.0.0-alpha.0"
edition = "2024"
rust-version = "1.95"
```

На старте v1.0 `members = []`. Крейт добавляется в members только когда его минимальный API landс и проходит базовую проверку.

## §7. Foundational patterns

### §7.1. Sans-I/O

Protocol-логика отделяется от I/O-транспорта физически — разные crates.

- **Protocol crate** (`bsql-pg-proto`): pure sync state-machine. Нет tokio. Нет sockets. Нет async. Методы:
  ```rust
  fn feed_bytes(&mut self, bytes: &[u8]) -> OutActions;
  fn push_command(&mut self, cmd: PgCommand) -> OutActions;
  ```
- **Driver crate** (`bsql-driver-postgres`): thin async wrapper (~100-200 LoC) вокруг socket + tokio task + channels.

**Зачем это делает §3 возможным:**

1. Cancellation-safety by construction. User future держит `oneshot::Receiver`. Drop future → drop receiver. Background task видит закрытый channel, **продолжает** драйвить state-machine до RFQ (чистое состояние), выбрасывает reply (никому не нужен), готов к next command.
2. Verifiability. Sync code — proptest 10⁵, Loom harness на ~100 LoC async wrapper, typestate — всё применимо. Async-mixed code всё это теряет.
3. Reuse. Тот же `PgProtocol` используется и sync и async wrapper'ами. v0.27 имел 5K LoC sync + 1.5K LoC async с дубликатом логики. v1.0: 1 state machine, 2 тонкие оболочки.

**Прецедент в Rust ecosystem:** `rustls`, `quiche`, `h2` используют этот паттерн. Ни один Rust SQL-драйвер до bsql этого не делал.

### §7.2. State-as-data (все correlators inline)

Каждый in-flight reply correlator живёт **внутри** варианта state enum, не в параллельном поле.

Плохо:
```rust
struct Protocol {
    state: State,
    pending_reply: Option<ReplyId>,  // параллельно
}
enum State { Idle, InFlight, ... }
```

Хорошо:
```rust
struct Protocol {
    state: State,
}
enum State {
    Idle,
    InFlight(ReplyId),  // inline
}
```

Transition из `InFlight(ReplyId)` требует move'нуть `ReplyId` куда-то (в action `DeliverReply` или `FailReply`). Компилятор отказывается собрать код который теряет данные варианта. Класс «lost in-flight reply on transition» → IMPOSSIBLE-BY-COMPILE.

Техника: `core::mem::take(&mut self.state)` — owned match. ProtoState: Default = Idle:

```rust
fn transition(&mut self) {
    let prev = core::mem::take(&mut self.state);
    match prev {
        State::InFlight(reply) => {
            // должен использовать reply — иначе compile error
            push_action(Action::DeliverReply { id: reply, ... });
            self.state = State::Idle;
        }
        _ => { ... }
    }
}
```

### §7.3. Typestate where it pays off

Zero-sized phantom markers encode inv'арианты в type params. PhantomData — 0 байт, 0 runtime cost.

Применяется где:
- Protocol transition chains имеют строгий порядок (Idle → SentParse → AwaitingParseComplete → …).
- Connection lifecycle states (Unconfigured, Configured, Connected, Closed).
- Transaction savepoint stack (depth encoded в type).

НЕ применяется где:
- Linear enum variants с явной match — `ProtoState` уже ловит это через state-as-data + exhaustive match.
- Overhead ergonomic превышает safety benefit (typestate с consume-self затрудняет composition; если enum дисциплина уже даёт tier 1, typestate избыточен).

Правило: typestate добавляется **только** когда даёт compile-time safety value недостижимую другими средствами.

### §7.4. Bounded types

`heapless::Vec<T, N>`, `heapless::String<N>`, `heapless::FnvIndexMap` — stack-allocated fixed-capacity аналоги. Применяются в sans-I/O core и hot-path:

- `ReadBuf` / `WriteBuf` — `heapless::Vec<u8, READ_BUF_CAP>` (4096 default; параметризуется const generic).
- StartupMessage / SCRAM message payload — `heapless::Vec<u8, 512>`.
- Identifier storage — `heapless::String<63>` (PG `NAMEDATALEN - 1`).
- Statement cache entries — `heapless::FnvIndexMap<u64, StmtEntry, 256>`.
- Pending replies map (в async wrapper) — можно `heapless`, можно regular HashMap (runtime тут).

**Значения OOM'ов в hot-path:** Bounded = Err на overflow, классифицируемый error. Unbounded = alloc failure = panic на большинстве аллокаторов. Bounded → IMPOSSIBLE OOM panic.

**Overflow handling:** `append` возвращает Err → `emit_classified_error` → state Errored + Action::CloseSocket. Client видит `ConnectionLost` на oneshot. Pool discards connection. Никаких silent пропусков.

**Спорные зоны:**
- Response buffer для large rows (>4KB). Решение: bounded per-frame, streaming handles large rows через multiple frames.
- Numeric decoders для очень длинных чисел. Решение: bounded или fallback на `alloc::String` с классифицируемой `DecodeTooLarge` ошибкой.

### §7.5. Sealed traits — с честностью

Sealed trait pattern:
```rust
pub mod private { pub trait Sealed {} }
pub trait Backend: private::Sealed + ... {}
```

**Честно:** это **tier 3 by audit**, не tier 1 by compile. `pub mod private` reachable внешним crate через `bsql_backend::private::Sealed`. Rust stable не даёт absolute cross-crate seal — сам tokio / rustls / serde этого не достигают.

Причина компромисса: наши собственные driver-crates (`bsql-driver-postgres`, etc.) — external relative to `bsql-backend`. Им нужен доступ к `Sealed` для `impl Sealed for PgBackend`. Private mod (non-pub) блокирует всех, включая нас.

**Митигация (tier 3):**
- Документация в lib.rs: «Do not implement. Doing so circumvents CREDO §0 audit boundary.»
- `cargo-vet` (§95) — dependency audit при появлении ecosystem'а.
- Commit review: PR добавляющий `impl bsql_backend::private::Sealed` — immediate flag.

Когда stable Rust (или edition 202X) даст absolute cross-crate seal — переходим на tier 1. До тех пор — openly tier 3.

### §7.6. Linear discipline (#[must_use] + Drop guards)

Rust не имеет native linear types (must-use-exactly-once). Approximation:

- `#[must_use = "..."]` на критичных типах (`JoinHandle`, `Transaction`, `CacheGuard`, ReplyIds) — warning/error при silent drop.
- Drop-guard RAII: `Transaction::Drop` → auto-rollback если не committed. `PoolGuard::Drop` → return to pool.
- Panic-on-drop ghosts в debug: `MustReply<T>` wrapper вокруг `oneshot::Sender<T>` — `debug_assert!` если drop'nulся без send (в release — silent, но compile-time `#[must_use]` уже поймал большинство).

### §7.7. Zero-copy lifetime threading

Row payloads — `&'buf [u8]` slices борrowed из response buffer, не owned `Vec<u8>`.

```rust
pub struct Row<'buf> { cols: &'buf [ColRef<'buf>] }
pub struct ColRef<'buf> { bytes: &'buf [u8] }
impl<'buf> ColRef<'buf> {
    pub fn as_str(&self) -> Result<&'buf str, Utf8Error>;
    pub fn as_i32(&self) -> Result<i32, DecodeError>;
    pub fn to_string(&self) -> Result<String, DecodeError>;
}
```

User iterating over rows holds a borrow; while held, buffer can't be reused — **compile-enforced by lifetimes**. User pays copy cost только когда extract'ит (`to_string`).

Many queries не extract — pagination, counts, EXISTS checks — просто look at values и move on. Для них — 0 alloc.

### §7.8. Capability tokens

Operations gated by unforgeable tokens. Token's existence = proof of prior validation.

```rust
pub struct PoolCap { _private: () }  // private ctor
impl<B: Backend> Pool<B> {
    fn cap(&self) -> PoolCap { PoolCap { _private: () } }
    pub fn acquire(&self, _: PoolCap) -> PoolGuard<'_, B>;
}
```

User не может сконструировать `PoolCap` самостоятельно — только через `&pool`. Acquire-without-pool-reference физически невозможно.

В bsql применяется для:
- Pool ownership verification (acquire requires &pool).
- Transaction-spawn permission (начать тx без pool токена нельзя).
- StreamHandle (consume row only if stream guard active).

### §7.9. Exhaustive match + `non_exhaustive_omitted_patterns`

Все наши enum'ы **не** `#[non_exhaustive]` для internal types — exhaustive match форсирован компилятором.

Public enum'ы (ошибки surface'емые user'у) — `#[non_exhaustive]` чтобы adding variant не ломал user-код. User обязан иметь catch-all `_ =>`.

`#[deny(non_exhaustive_omitted_patterns)]` (nightly, TODO когда стабилизируется) — ловит пропуск варианта *upstream's* `#[non_exhaustive]` enum в нашем match.

### §7.10. Owned-or-borrowed buffer enums

Когда нужны и `&'static [u8]` (compile-time const) и `heapless::Vec<u8, N>` (runtime-built), но Action<'buf> лайфтаймы конфликтуют с re-borrow loops — newtype enum:

```rust
pub enum SendBuf {
    Static(&'static [u8]),
    Owned(heapless::Vec<u8, MAX_LEN>),
}
impl SendBuf { fn as_bytes(&self) -> &[u8]; }
```

`large_enum_variant` clippy warning → `#[expect(..., reason = "...")]` с обоснованием (no_std + no_alloc precludes Box). Inline-bounded IS the point.

### §7.11. No external verifiers

Kani, Verus, Prusti, Flux — **out of scope**. Причины:

- Partial coverage (ограниченное подмножество Rust).
- MSRV-binding (к release tool'а, не самого языка).
- Attached modules — safety обретает separate-tool dependency.
- Integration cost versus return уже доказан negative для похожих проектов.

Safety мы получаем **от самого Rust**: types, traits, lifetimes, macros, clippy, sans-I/O separation, bounded types, Loom, proptest. Всё integral — часть codebase, compiled user's own rustc'ом, reviewable as normal Rust.

Когда external-verifier landscape сменится — решение пересматривается. До тех пор — no.

---

# Часть III — Крейты (детально)

## §8. bsql (фасад)

**Role:** user-facing entry point. Zero-logic crate — только `pub use` реэкспорты.

**Публикуемая поверхность:**
```rust
// Macros
pub use bsql_macros::{query, pg_enum, sort, test, connect};

// Runtime types
pub use bsql_core::{Pool, Transaction, Listener, Notification, BsqlError, Sensitive};

// Backend handles (generic form hidden; facade type aliases)
pub type PgPool = bsql_core::Pool<bsql_driver_postgres::PgBackend>;
pub type SqlitePool = bsql_core::Pool<bsql_driver_sqlite::SqliteBackend>;
pub type PgTransaction<'p> = bsql_core::Transaction<'p, bsql_driver_postgres::PgBackend>;
pub type SqliteTransaction<'p> = bsql_core::Transaction<'p, bsql_driver_sqlite::SqliteBackend>;

// Error variants for pattern matching
pub use bsql_core::error::{Pool as PoolError, Query as QueryError, Decode as DecodeError, Connect as ConnectError};

// Feature-gated type re-exports (time, uuid, decimal, chrono)
#[cfg(feature = "time")]   pub use time;
#[cfg(feature = "uuid")]   pub use uuid;
#[cfg(feature = "chrono")] pub use chrono;
#[cfg(feature = "decimal")] pub use rust_decimal;
```

**Никаких** собственных типов. Никаких `trait`'ов. Никаких функций. Facade — buffer между user'ом и implementation crates; user не знает в каком crate живёт `Pool`.

**Cargo features:**
```toml
[features]
default = ["postgres", "async"]
postgres = ["dep:bsql-driver-postgres"]
sqlite = ["dep:bsql-driver-sqlite"]
async = ["bsql-driver-postgres/async", "bsql-core/async"]
# sync mode — без tokio
sync = []
time = ["bsql-core/time", "bsql-macros/time"]
uuid = ["bsql-core/uuid", "bsql-macros/uuid"]
chrono = ["bsql-core/chrono", "bsql-macros/chrono"]
decimal = ["bsql-core/decimal", "bsql-macros/decimal"]
explain = ["bsql-macros/explain"]
detect-n-plus-one = ["bsql-core/detect-n-plus-one"]
```

## §9. bsql-macros

**Role:** proc-macro crate. Reads SQL at compile-time, validates against real DB или offline cache, generates typed Rust code.

Глубокий разбор — **Часть V** (весь раздел).

**Dep surface (runtime):**
- `syn` (full / parsing) — parse macro input.
- `quote` — emit generated code.
- `proc-macro2` — span manipulation.
- `bitcode` — read/write offline cache files (`.bsql/queries/*.bitcode`).
- `sha2` — schema fingerprint.
- `rapidhash` — query hash → stmt name.
- (optional, for online mode) minimal DB client — see §28.

**НЕ зависит** от `bsql-core`, `bsql-backend`, `bsql-driver-*`. Proc-macro crate видит только типы-через-имена; runtime symbols referenced в generated code, не линкуются.

## §10. bsql-arena

**Role:** internal bump allocator для row data.

**API (internal):**
```rust
pub struct Arena { ... }
impl Arena {
    pub fn new() -> Self;
    pub fn reset(&mut self);  // free all, keep capacity
    pub fn alloc_bytes(&mut self, bytes: &[u8]) -> &[u8];  // returns slice into arena
    pub fn alloc_str(&mut self, s: &str) -> &str;
    // ... etc
}

// Thread-local recycling
pub fn acquire() -> PooledArena;
pub fn release(arena: PooledArena);
```

**Инвариант:** arena — implementation detail. User не видит `Arena` type. `PooledArena` через thread-local — до 4 arena на тред, LIFO reuse. Zero-malloc на second-query-on-thread.

**Dep:** none (нужен `alloc` для growth; `no_std + alloc`).

**Размер:** ~500 LoC ожидается после rewrite (v0.27 было 1.7K; упрощаем).

## §11. bsql-core

**Role:** generic runtime types — `Pool<B>`, `Transaction<'pool, B>`, `Listener`, error types, bounded helpers.

**Публикуемая поверхность:**
```rust
pub struct Pool<B: Backend> { ... }
impl<B: Backend> Pool<B> {
    pub async fn connect(url: &str) -> Result<Self, BsqlError>;
    pub fn builder() -> PoolBuilder<B>;
    pub async fn acquire(&self) -> Result<PoolGuard<'_, B>, BsqlError>;
    pub async fn close(self, timeout: Option<Duration>) -> Result<(), BsqlError>;
    pub fn status(&self) -> PoolStatus;
    pub async fn raw_execute(&self, sql: &str) -> Result<u64, BsqlError>;
    pub async fn raw_query(&self, sql: &str) -> Result<Vec<RawRow>, BsqlError>;
    pub async fn raw_query_params(&self, sql: &str, params: &[&(dyn Encode + Sync)]) -> Result<Vec<RawRow>, BsqlError>;
    pub fn set_warmup_sqls(&self, iter: impl IntoIterator<Item = &str>);
}

pub struct PoolBuilder<B: Backend> { ... }
impl<B: Backend> PoolBuilder<B> {
    pub fn url(self, url: &str) -> Self;
    pub fn max_size(self, n: usize) -> Self;
    pub fn acquire_timeout(self, d: Duration) -> Self;
    pub fn max_lifetime(self, d: Duration) -> Self;
    pub fn stale_timeout(self, d: Duration) -> Self;
    pub fn min_idle(self, n: usize) -> Self;
    pub fn replica_url(self, url: &str) -> Self;
    pub fn replica_max_size(self, n: usize) -> Self;
    pub fn max_stmt_cache_size(self, n: usize) -> Self;
    pub fn singleflight(self, enabled: bool) -> Self;
    pub fn warmup(self, sqls: &[&str]) -> Self;
    pub fn build(self) -> Result<Pool<B>, BsqlError>;
}

pub struct PoolGuard<'pool, B: Backend> { ... }
impl<'pool, B: Backend> PoolGuard<'pool, B> {
    pub async fn begin(self) -> Result<Transaction<'pool, B>, BsqlError>;
    pub async fn begin_with(self, level: IsolationLevel) -> Result<Transaction<'pool, B>, BsqlError>;
    // query execution methods generated by the macro via this guard
}

pub struct Transaction<'pool, B: Backend> { ... }
impl<'pool, B: Backend> Transaction<'pool, B> {
    pub async fn commit(self) -> Result<(), BsqlError>;
    pub async fn rollback(self) -> Result<(), BsqlError>;
    pub async fn savepoint(&mut self, name: &str) -> Result<(), BsqlError>;
    pub async fn release(&mut self, name: &str) -> Result<(), BsqlError>;
    pub async fn rollback_to(&mut self, name: &str) -> Result<(), BsqlError>;
}

pub struct Listener { ... }
impl Listener {
    pub async fn connect(url: &str) -> Result<Self, BsqlError>;
    pub async fn listen(&mut self, channel: &str) -> Result<(), BsqlError>;
    pub async fn unlisten(&mut self, channel: &str) -> Result<(), BsqlError>;
    pub async fn recv(&mut self) -> Result<Notification, BsqlError>;
    pub async fn recv_timeout(&mut self, d: Duration) -> Result<Notification, BsqlError>;
    pub fn try_recv(&mut self) -> Result<Option<Notification>, BsqlError>;
}

pub enum BsqlError {
    Pool(PoolErrorKind),
    Query(QueryErrorKind),
    Decode(DecodeErrorKind),
    Connect(ConnectErrorKind),
    OutOfMemory { bytes_requested: usize },
    UserCallbackPanicked { payload: String },
    SchemaVersionMismatch { compile: [u8; 32], runtime: [u8; 32] },
    UnsupportedServerVersion { server: u32, supported_range: &'static str },
    #[non_exhaustive]
    Other,
}

pub struct Sensitive<T> { ... }  // zeroize on drop, REDACTED Debug

pub enum IsolationLevel { ReadCommitted, RepeatableRead, Serializable }

pub struct Notification { channel: String, payload: String, pid: i32 }

pub struct RawRow { ... }  // untyped text rows from raw_query

pub trait Encode { ... }  // user-facing encoder trait (for raw_query_params)
```

**Generic pool архитектура:** `Pool<B>` holds `Vec<PoolSlot<B>>`, each slot hosts `Client<B>` (от `bsql-backend`). Slot lifecycle: acquire → use → release. LIFO, fail-fast по умолчанию, opt-in `acquire_timeout` для burst tolerance.

## §12. bsql-backend

**Role:** generic Backend trait + Client<B> channel architecture.

```rust
pub trait Backend: private::Sealed + Sized + Send + Sync + 'static {
    type Protocol: Send + 'static;
    type Command: Send + 'static;
    type Error: BackendError + Send + Sync + 'static;
    type Config: Send + Sync + 'static;
    type Transport: Send + 'static;

    fn new_protocol() -> Self::Protocol;
    fn connect(cfg: &Self::Config) -> impl Future<Output = Result<Self::Transport, Self::Error>> + Send;
    fn run_io(proto: Self::Protocol, transport: Self::Transport, cmd_rx: mpsc::Receiver<Self::Command>) -> impl Future<Output = ()> + Send;
    fn ping_command(reply: oneshot::Sender<Result<(), Self::Error>>) -> Self::Command;
}

pub trait BackendError: std::error::Error {
    fn is_fatal_connection(&self) -> bool;
}

pub struct Client<B: Backend> { ... }
impl<B: Backend> Client<B> {
    #[must_use = "hold JoinHandle for deterministic exit observation"]
    pub fn spawn(proto: B::Protocol, transport: B::Transport) -> (Self, JoinHandle<()>);
    pub async fn send(&self, cmd: B::Command) -> Result<(), SendError<B::Command>>;
    pub fn try_send(&self, cmd: B::Command) -> Result<(), TrySendError<B::Command>>;
    pub fn is_connected(&self) -> bool;
    pub async fn ping(&self) -> Result<(), PingError<B>>;
}

pub mod private { pub trait Sealed {} }

pub const CLIENT_CMD_CHANNEL_CAP: usize = 16;
```

**Invariants (see §7.4 для details):**
- Every Command processed in `run_io` completes before next `recv()`.
- Task exits cleanly on `cmd_rx.recv() == None` (all Clients dropped).
- Fatal I/O / FFI → `is_fatal_connection = true`.
- No busy-wait, no panics in `run_io`.

## §13. bsql-pg-proto

**Role:** pure sync PostgreSQL wire-protocol state machine.

Глубокий разбор — **Часть IV** (§17-§22).

**High-level:**
```rust
pub struct PgProtocol { ... }
impl PgProtocol {
    pub const fn new() -> Self;  // starts in Idle; handshake команда Startup
    pub fn state(&self) -> &ProtoState;
    pub fn feed_bytes(&mut self, bytes: &[u8]) -> OutActions;
    pub fn push_command(&mut self, cmd: PgCommand) -> OutActions;
}

pub enum PgCommand {
    Ping { reply: ReplyId },
    Startup { user: Name, database: Name, credentials: Credentials, reply: ReplyId },
    Query { sql: Arc<str>, hash: u64, params: Vec<EncodedParam>, reply: ReplyId },
    Execute { ... },
    Begin { level: IsolationLevel, reply: ReplyId },
    Commit { reply: ReplyId },
    Rollback { reply: ReplyId },
    Listen { channel: Arc<str>, reply: ReplyId },
    Unlisten { channel: Arc<str>, reply: ReplyId },
    QueryStream { ..., rows_tx: mpsc::Sender<...>, done_tx: oneshot::Sender<...> },
    CancelCurrent { query_id: QueryId, reply: ReplyId },
    Terminate { reply: ReplyId },
}

pub enum ProtoState { ... state-as-data variants ... }
pub enum Action { SendBytes(SendBuf), DeliverReply, FailReply, CloseSocket }
pub type OutActions = heapless::Vec<Action, MAX_ACTIONS_PER_CALL>;
```

**Constraint:** `#![no_std]`, никаких `alloc`. Bounded storage. Единственная runtime dep — `heapless`.

**Size estimate:** ~2-3 KLOC после полного wire protocol (startup + SCRAM + query + streaming + COPY + listen).

## §14. bsql-driver-postgres

**Role:** PgBackend impl + async run_io + auth (SCRAM) + TLS + binary codec.

**Содержит:**
- `PgBackend` struct + `impl Backend for PgBackend`.
- `run_io` — ~100-150 LoC tokio select loop.
- `Config` — parsed URL (host, port, user, database, sslmode, channel_binding, application_name, …).
- `Credentials` — password, mTLS cert, SCRAM state.
- SCRAM-SHA-256 + channel binding (SCRAM-SHA-256-PLUS).
- TLS — rustls via ring crypto provider, webpki-roots.
- Binary wire codec (encode / decode for native types).
- Pending-replies map `HashMap<ReplyId, oneshot::Sender<...>>` — async wrapper state.

**Deps:** tokio (full), rustls (default=ring), webpki-roots, tokio-rustls, rustls-pemfile, rapidhash, и т.д.

## §15. bsql-driver-sqlite

**Role:** SqliteBackend + FFI + spawn_blocking shim.

**Архитектура:** SQLite inherently sync. Async API — через `tokio::task::spawn_blocking`. Command очередь одна; `run_io` — loop over cmd_rx, каждая command → spawn_blocking { ffi_call }.

- FFI layer (`src/ffi.rs`) — **единственный** file с `unsafe`. Каждый `unsafe { ... }` с SAFETY-commentary.
- Остальные modules — `#![forbid(unsafe_code)]` module-level.
- Handle lifecycle (`DbHandle`, `StmtHandle`) — RAII Drop calls `sqlite3_close_v2` / `sqlite3_finalize` exactly once.
- Pool: 1 writer + N readers (WAL mode default). Writers serialized; readers parallel.

---

# Часть IV — Wire layer детально

## §16. PgProtocol shape

```rust
pub struct PgProtocol {
    state: ProtoState,
    read_buf: ReadBuf,
    write_buf: WriteBuf,
    stmt_cache: StmtCache,  // prepared statement LRU
    session_params: SessionParams,  // from ParameterStatus (server_version, ...)
    schema_fingerprint: Option<[u8; 32]>,
    _not_sync: PhantomData<Cell<()>>,  // !Sync by construction
}
```

**ReadBuf / WriteBuf** — newtype-sealed `heapless::Vec<u8, CAP>`. API exposes ONLY: `append()`, `unread()`, `clear()`, `advance()`. Methods panic-on-misuse (`insert`, `resize`, `drain`, indexing `[i]`) physically absent.

**`!Sync`** via `PhantomData<Cell<()>>`. Concurrent race на protocol state — STRUCTURALLY UNREACHABLE: only one task can own `&mut PgProtocol` at a time, compiler enforces.

## §17. State enum (state-as-data)

```rust
pub enum ProtoState {
    Idle,
    ConnectingStartup(ReplyId),          // sent StartupMessage, awaiting auth
    ConnectingScram { reply: ReplyId, step: ScramStep, client_nonce: Sensitive<[u8; 24]>, server_data: Option<ScramServerFirst> },
    ConnectingPostAuthWaitKey(ReplyId),  // AuthOk received, awaiting BackendKeyData
    ConnectingPostAuthHaveKey { reply: ReplyId, pid: i32, secret_key: i32 },
    AwaitingPingReply(ReplyId),
    AwaitingQueryReply { reply: ReplyId, hash: u64, columns: ColumnMeta },
    StreamingRows { stream: StreamHandle, hash: u64, columns: ColumnMeta },
    InTransaction { level: IsolationLevel, depth: u8 /* savepoint stack */ },
    Errored(ProtocolError),
    Closed,
}
```

**Каждый variant** inline carries correlators. Transition requires `match prev { Variant(data) => consume(data); self.state = next }`. Compiler enforces — бага «lost correlator» не существует.

**Variants добавляются** когда их путь реализуется. Manufactured-future variants (без кода входа/выхода) — запрещены §4.6.

## §18. Command enum

`PgCommand` — см. §13. Each variant:
- Carries reply correlator (`ReplyId` abstract handle; wrapper переводит в `oneshot::Sender`).
- Is `Send + 'static`.

`ReplyId` — opaque `u64` handle. Protocol не знает про tokio. Wrapper maintains `HashMap<ReplyId, oneshot::Sender<T>>`, dispatches `Action::DeliverReply { id, value }` через него.

## §19. Action enum

```rust
pub enum Action {
    SendBytes(SendBuf),
    DeliverReply { id: ReplyId, value: Reply },
    FailReply { id: ReplyId, cause: ProtocolError },
    StreamRow { stream: StreamHandle, row: OwnedRow },
    CloseSocket,
}
pub enum SendBuf {
    Static(&'static [u8]),                        // compile-time const
    Owned(heapless::Vec<u8, MAX_OWNED_SEND_LEN>), // runtime-built
}
pub type OutActions = heapless::Vec<Action, MAX_ACTIONS_PER_CALL>;
```

**Per-method push budget** — const per operation + `const _ : () = assert!(MAX >= MAX_PUSHED_BY_…)`. Overflow push — impossible at compile.

**SendBuf rationale** см. §7.10. `large_enum_variant` clippy → `#[expect(...)]` с reason.

## §20. Frame parsing (pure function)

```rust
fn parse_header(unread: &[u8]) -> HeaderParse {
    match unread {
        [] => HeaderParse::Empty,
        [_] | [_, _] | [_, _, _] | [_, _, _, _] => HeaderParse::Incomplete,
        [tag, l0, l1, l2, l3, ..] => {
            let declared = u32::from_be_bytes([*l0, *l1, *l2, *l3]);
            if declared < 4 { return HeaderParse::MalformedLength { declared }; }
            if usize::try_from(declared).ok().is_none_or(|n| n > MAX_FRAME_LEN) {
                return HeaderParse::FrameTooLarge { declared };
            }
            HeaderParse::Ok { tag: *tag, declared_len: declared, total_len: declared as usize + 1 }
        }
    }
}
```

Pure function — no state mutation, no I/O. Testable in isolation. Early-reject `FrameTooLarge` (declared_len > READ_BUF_CAP) — DoS via length amplification STRUCTURALLY UNREACHABLE.

## §21. Dispatch (typed outcome)

```rust
enum DispatchOutcome {
    Advanced { new_state: ProtoState, by: usize },  // advance read_buf + replace state
    Errored,                                          // state already moved to Errored; actions pushed
}
```

Pattern match `(prev, header.tag)` exhaustive over ALL (state, tag) combinations. Unlisted = compile error.

Каждый arm либо `Advanced` либо `Errored`. Separation means outer feed_bytes loop can't accidentally advance on errored path — no silent corruption.

## §22. run_io (~100-200 LoC async wrapper)

```rust
pub async fn run_io(mut proto: PgProtocol, mut stream: TcpStream, mut cmd_rx: mpsc::Receiver<PgCommand>) {
    let mut pending: PendingReplies = PendingReplies::new();
    let mut read_chunk = [0u8; READ_CHUNK_SIZE];
    loop {
        tokio::select! {
            biased;
            // 1. Command from user (highest prio for latency).
            cmd = cmd_rx.recv() => {
                let Some(cmd) = cmd else { break; };  // all Clients dropped
                pending.register(&cmd);
                let actions = proto.push_command(cmd);
                dispatch_actions(&mut stream, &mut pending, actions).await;
            }
            // 2. Socket readable.
            n = stream.read(&mut read_chunk) => {
                let n = match n { Ok(n) if n > 0 => n, _ => { transport_error(&mut pending).await; break; } };
                let actions = proto.feed_bytes(&read_chunk[..n]);
                dispatch_actions(&mut stream, &mut pending, actions).await;
            }
        }
        if matches!(proto.state(), ProtoState::Errored(_) | ProtoState::Closed) { break; }
    }
    graceful_close(&mut stream, proto).await;
    fail_all_pending(&mut pending);
}
```

**Свойства:**
- `biased` select — command-channel priority, avoid starving.
- No spin-loop — every arm awaitable.
- No panic — caller panics logged, task exits, pending replies fail classified.
- Loom harness: concurrent interleaving check тут (§92).

---

# Часть V — Макросы детально

## §25. Макро-CREDO (внутренний этос)

Macros — самый opaque слой для user'а. «Магия». CREDO §0 внутри macros формулируется резче:

1. **Генерируемый код проходит тот же forbid-bundle** что core crate'ы. Нет лазеек. Generated `unwrap` — bug.
2. **Error messages — first-class feature.** Плохое сообщение = bug. Quality measured: поинтер на точный token (не на macro-invocation); Levenshtein suggestion; ссылка на документацию если applicable.
3. **Определённость.** `query!(same_sql_twice)` compiles идентично каждый раз. `bitcode` cache — deterministic сериализация. Hash stmt names — deterministic.
4. **Zero silent fallback.** Validation fails — compile error. Нет «если не получилось, попробуем как raw_query». Fail-loud.
5. **Online / offline inditinguishable in output.** Compile с live PG и с offline cache'ем → identical generated code. `cache miss` в strict offline mode → compile error.
6. **Minimum allocation at macro-time.** Macro expansion runs во время cargo build на developer machine — должно быть быстро. `bitcode` cache load 50 queries ≈ 100μs vs JSON ≈ 5ms.

## §26. Единственный макрос: `query!`

**User surface:**

```rust
let rows = bsql::query!(
    "SELECT id, login FROM users WHERE id = $id: i32"
).fetch_all(&pool).await?;
```

Macro expands в:
```rust
{
    // hidden generated struct
    struct __BsqlQuery_abc123 {
        pub id: i32,
        pub login: String,
    }
    let __stmt_hash = 0xabc123_u64;
    let __stmt_name = "s_abc123def456_0123";
    let __params = [(&id as &(dyn Encode + Sync))];
    __exec(&pool, __stmt_hash, __stmt_name, __params, decode_row_abc123)
}
```

Где:
- `__BsqlQuery_abc123` — generated struct с типами из `pg_catalog`.
- `__stmt_hash` — `rapidhash(sql_text)`.
- `__stmt_name` — `s_{hex16}_{variant_index}` (deterministic, stable across builds).
- `decode_row_abc123` — generated decoder для этой query.

**Execution methods on query result (exposed via trait / methods):**
```rust
.fetch_all(&pool).await    -> Vec<Row>
.fetch_one(&pool).await    -> Row       // errors if 0 or >1 rows
.fetch_optional(&pool).await -> Option<Row>
.execute(&pool).await      -> u64       // rows affected (INSERT/UPDATE/DELETE)
.fetch_stream(&pool).await -> QueryStream
.for_each(&pool, |row| { ... }).await
.for_each_map(&pool, |row| map_fn(row)).await
.defer(&mut tx).await      // buffer in transaction
.rows(iter).execute(&pool).await  // COPY FROM STDIN only
```

**Macro generates code that dispatches to the right execute-method based on SQL shape:**
- `SELECT` → `.fetch_all` / `.fetch_one` / `.fetch_optional` / `.fetch_stream` / `.for_each` / `.defer`.
- `INSERT` / `UPDATE` / `DELETE` → `.execute` / `.fetch_all` (если есть `RETURNING`).
- `COPY … FROM STDIN` → `.rows(iter).execute` only.
- Multi-statement → **compile error**. `raw_execute` для DDL.

Compile error messages — см. §42.

## §27. Pipeline компиляции

```
query!("...") invocation
       │
       ▼
  parse macro input (syn)
       │
       ▼
  extract SQL text + parameters ($name: Type) + optional clauses [ ... ]
       │
       ▼
  compute query hash = rapidhash(normalized sql)
       │
       ▼
  ┌─────────────────────────┐
  │  MODE SELECTION (§28)   │
  │  online vs offline      │
  └───────────┬─────────────┘
              │
       ┌──────┴──────┐
       ▼             ▼
  ONLINE           OFFLINE
  connect DB       read .bsql/queries/{hash}.bitcode
  run PREPARE      deserialize Validated struct
  run DESCRIBE     check schema_fingerprint == cached
  extract types
  compute fingerprint
       │
       └──────┬──────┘
              ▼
    Validated { columns, params, nullability, ... }
              │
              ▼
    generate Rust code (quote)
    emit struct + decoder + execute call
              │
              ▼
    (online) write Validated to .bsql/queries/{hash}.bitcode
```

Каждая стадия может fail — fail-loud diagnostics.

## §28. Online mode

Активен когда:
- `BSQL_DATABASE_URL` set.
- `BSQL_OFFLINE` НЕ set (или `= false`).

Macro connects to live DB **at compile time** (during `cargo build`). Minimal DB client inside proc-macro crate:
- TCP connect.
- StartupMessage (username / database from URL).
- SCRAM-SHA-256 auth (TLS optional).
- Run `PARSE` + `DESCRIBE` on query.
- Extract column types (OIDs), nullability (`pg_attribute.attnotnull`), parameter types.
- Serialize `Validated { ... }` to `.bsql/queries/{hash}.bitcode`.
- Append hash to `.bsql/queries/.manifest` under exclusive file lock.

**Why not reuse `bsql-driver-postgres`?** Circular dep — driver depends on macros for its own tests. Macro crate embeds **minimal** PG client (~500-800 LoC), subset of full driver (no pool, no streaming, no TLS for simplicity — or optional TLS).

**SCRAM implementation** в macros — share module с `bsql-driver-postgres` via `bsql-pg-proto`'s SCRAM state machine? Open question — see §117 Q-3.

## §29. Offline mode

Активен когда:
- `BSQL_OFFLINE=true`, OR
- `BSQL_OFFLINE` НЕ set AND `BSQL_DATABASE_URL` НЕ set AND `.bsql/queries/` exists.

Macro reads:
- `.bsql/queries/{hash}.bitcode` — deserialize to `Validated`.
- `.bsql/queries/.manifest` — check hash present.

If cache miss:
- `BSQL_OFFLINE=true` → **compile error**: `query not in offline cache`.
- Convenience mode → suggest `cargo build` with `BSQL_DATABASE_URL` set.

Teammates / CI: commit `.bsql/queries/` to git. CI builds with `BSQL_OFFLINE=true`, zero DB required.

## §30. Schema fingerprint

```rust
fn compute_schema_fingerprint(catalog: &CatalogSnapshot) -> [u8; 32] {
    let mut hasher = sha2::Sha256::new();
    for table in catalog.tables_sorted() {
        hasher.update(table.name.as_bytes());
        for column in table.columns_sorted() {
            hasher.update(column.name.as_bytes());
            hasher.update(&column.type_oid.to_le_bytes());
            hasher.update(&[column.not_null as u8]);
        }
    }
    for typ in catalog.types_sorted() {
        hasher.update(&typ.oid.to_le_bytes());
        hasher.update(typ.name.as_bytes());
    }
    hasher.finalize().into()
}
```

**Инвариант:** sorted inputs → deterministic hash. Тот же schema → тот же hash.

**Runtime check в Pool::connect:**
```rust
let runtime_fingerprint = raw_query("SELECT bsql_compute_schema_fingerprint()");
if runtime_fingerprint != compile_fingerprint {
    return Err(BsqlError::SchemaVersionMismatch { ... });
}
```

Detects drift between compile-time cache и runtime DB schema. Не warning — error. User должен rebuild.

## §31. Offline cache integrity

- **Atomic write:** `bitcode::encode → tempfile → fsync → rename`. Non-atomic partial write невозможен.
- **Manifest append under file lock:** `fs2::FileExt::lock_exclusive` — parallel `cargo build` (workspace rustc spawn) не race'ят manifest.
- **Bitcode envelope версионирован:** `struct BitcodeEnvelope { schema_version: u32, payload: Validated }`. Новый bsql version → fail с clear "upgrade/clear cache".
- **`bsql verify` CLI:** check integrity — каждый hash в manifest имеет matching bitcode файл + bitcode deserializes. Exit 1 on breakage.
- **`bsql clean` CLI:** wipe cache; next cargo build repopulates.
- **Stale entries:** auto-clean НЕ делается. Detecting "build finished" reliably из proc-macro невозможно (cargo fans rustc per-crate). Past auto-cleanup corrupted production builds. Manual `bsql clean` — accepted tradeoff.

## §32. Type inference

```rust
pub struct Validated {
    pub columns: Vec<ColumnMeta>,
    pub parameters: Vec<ParamMeta>,
    pub query_kind: QueryKind,  // SELECT / INSERT / UPDATE / DELETE / COPY
    pub has_returning: bool,
    pub variants: Vec<SqlVariant>,  // expanded optional clauses
    pub schema_fingerprint: [u8; 32],
    pub bsql_version: &'static str,
}

pub struct ColumnMeta {
    pub name: String,
    pub type_oid: u32,
    pub not_null: bool,
    pub generated: Option<GenerationRule>,  // GENERATED ALWAYS AS ...
}

pub struct ParamMeta {
    pub placeholder_name: Option<String>,  // $id (named) or None ($1)
    pub declared_rust_type: Option<String>,  // from $id: i32
    pub expected_oid: u32,
}
```

Mapping OID → Rust type via well-known table (int4 → `i32`, text → `String`, etc.). Unknown OIDs → error с suggestion «enable feature flag for decimal / uuid / time / chrono».

## §33. Nullability inference (50+ patterns)

Для каждой колонки в `SELECT ...`:

- **Simple column:** OID + `pg_attribute.attnotnull`. NOT NULL column → `T`, otherwise `Option<T>`.
- **Expressions (computed):** по умолчанию `Option<T>`. Overrides per-pattern:

  | Pattern | Inferred |
  |---|---|
  | `COUNT(*)` / `COUNT(col)` | `i64` NOT NULL |
  | `COALESCE(nullable, NOT_NULL)` | `String` NOT NULL |
  | `COALESCE(nullable, nullable)` | `Option<String>` |
  | `EXISTS(subquery)` | `bool` NOT NULL |
  | `CURRENT_TIMESTAMP` / `NOW()` | NOT NULL |
  | integer literal `42` | NOT NULL |
  | string literal `'hello'` | NOT NULL |
  | `col::text` (cast target) | NOT NULL if source is NOT NULL |
  | `CASE WHEN ... THEN a ELSE b END` | branch-union: NOT NULL if all branches NOT NULL |
  | `ROW_NUMBER()` / `DENSE_RANK()` / `RANK()` | `i64` NOT NULL |
  | `SUM(col)` | `Option<T>` (SUM over empty set = NULL) |
  | `SUM(NOT NULL col)` over non-empty | `Option<T>` — empty result still NULL |
  | `MIN` / `MAX` over nullable | `Option<Option<T>>` |
  | `LEFT JOIN ... t.col` | `Option<T>` regardless of `NOT NULL` |
  | `RIGHT JOIN ... t.col` | `Option<T>` |
  | `FULL OUTER JOIN ... t.col` | `Option<T>` |
  | Window functions over partition | typically NOT NULL; per-function |
  | `UNNEST($arr)` | element type (`T` or `Option<T>` based on array inner null) |
  | `array_agg(col)` | `Option<Vec<...>>` |
  | `jsonb_build_object(...)` | NOT NULL |
  | `row_to_json(...)` | NOT NULL |

50+ patterns — полный список ведётся в исходнике с юнит-тестами per pattern. User facing: если паттерн не распознан, default = `Option<T>` (safe). User compensates с `.unwrap()` where needed — **предпочтительно** silent panic.

**Философия §4.8:** when in doubt → `Option<T>`. Spurious `.unwrap()` at call site дешёвый to fix; silent runtime crash дорогой.

## §34. Optional clauses (dynamic queries)

```rust
let tickets = bsql::query!(
    "SELECT id, title FROM tickets WHERE deleted_at IS NULL
     [AND department_id = $dept: Option<i64>]
     [AND assignee_id = $assignee: Option<i64>]
     [AND status = ANY($statuses: Option<&[String]>)]"
).fetch_all(&pool).await?;
```

Macro expands в 2^N вариантов SQL (N optional clauses, 2^N combinations). **Каждый вариант валидируется** против PG через `PREPARE` отдельно. Each gets own stmt name.

Runtime dispatcher — `match` bitflag:
```rust
let variant = (dept.is_some() as u8) | ((assignee.is_some() as u8) << 1) | ((statuses.is_some() as u8) << 2);
match variant {
    0b000 => exec_variant_0(...),
    0b001 => exec_variant_1(...),
    ...
    0b111 => exec_variant_7(...),
}
```

Jump table — <5ns dispatch overhead.

**Maximum:** 10 optional clauses (1024 variants). Beyond — compile error: «too many optional clauses, consider refactoring».

**Compile time cost:** N optional clauses generates 2^N `PREPARE` roundtrips при online mode. 6+ clauses — noticeable compile-time увеличение. Offline mode — only bitcode reads, no cost.

## §35. Sort enums

```rust
#[bsql::sort]
enum TicketSort {
    #[sql("created_at DESC")] Newest,
    #[sql("created_at ASC")]  Oldest,
    #[sql("priority DESC, created_at DESC")] PriorityThenNewest,
}

let tickets = bsql::query!(
    "SELECT id, title FROM tickets ORDER BY $[sort: TicketSort] LIMIT $limit: i64"
).fetch_all(&pool).await?;
```

Каждый variant's SQL — validated at compile time против PG. Enum exhaustive — no default case, no "unknown" sort. Runtime dispatch — `match` jump table.

**Invariant:** каждая sort SQL — **static**. Не `format!`, не concat, не runtime. Pure compile-time substitution.

## §36. UNNEST bulk insert

```rust
let titles: Vec<String> = (0..1000).map(|i| format!("ticket_{i}")).collect();
let user_ids: Vec<i32> = (0..1000).map(|i| (i % 10) + 1).collect();

let inserted = bsql::query!(
    "INSERT INTO tickets (title, created_by_user_id)
     SELECT * FROM UNNEST($titles: &[String], $user_ids: &[i32])
     RETURNING id"
).fetch_all(&pool).await?;
```

- **1 round-trip** for any N.
- Param count = columns (2), NOT columns × rows (2×1000) — obviates PG's 65,535 param limit.
- Full SQL power: `RETURNING`, `ON CONFLICT`, `WHERE`, triggers — works because one statement.
- Array length parity: macro emits runtime check (titles.len() == user_ids.len()) before send. Mismatch → `BsqlError::ArrayLengthMismatch`, before wire traffic.

**Bulk pattern selection:**
- N = 1..~50: regular `INSERT ... VALUES` в `tx.defer()` loop (pipelined).
- N = ~10..~50K: UNNEST (recommended).
- N ≥ ~50K: `COPY FROM STDIN` (§37).

Thresholds approximate, dependent on column count + data size.

## §37. COPY FROM STDIN (only for massive bulk)

```rust
let rows: Vec<(String, i32)> = (0..100_000)
    .map(|i| (format!("ticket_{i}"), (i % 10) + 1))
    .collect();

let inserted = bsql::query!("COPY tickets (title, created_by_user_id) FROM STDIN")
    .rows(rows)
    .execute(&pool).await?;
```

- Tuple type `(String, i32)` enforced at compile — must match target columns в порядке.
- Wrong arity / wrong type → compile error.
- No `RETURNING` (COPY doesn't produce rows).
- No `ON CONFLICT`.
- Not supported inside `&mut Transaction` (use `&pool` или `&mut PoolConnection`).

Macro dispatches к COPY binary protocol based on SQL shape (`.starts_with("COPY")`).

## §38. Compile-time EXPLAIN (feature `explain`)

```toml
bsql = { version = "1.0", features = ["explain"] }
```

С этой фичей macro runs `EXPLAIN` на каждом query during `cargo build`. Результат:

- Embedded as doc comment on generated struct — hover в IDE показывает plan.
- **Seq Scan warnings:** если plan содержит seq-scan на table >1000 rows (configurable through `BSQL_EXPLAIN_SEQ_SCAN_THRESHOLD`):
  ```
  warning: [bsql] Seq Scan on "orders" (est. 50000 rows) — consider adding an index
    --> src/handlers.rs:42:18
  ```
- **Missing index hints:** если plan contains "Index Scan using" — OK; otherwise hint.

Development-only — disable в CI и release. Slow (adds ~50ms per query compile-time).

## §39. Compile-time URL validation (`Pool::connect!`)

```rust
let pool = bsql::Pool::connect!("postgres://bsql:bsql@localhost/mydb").await?;
```

Macro parses URL at compile-time. Invalid URL = compile error, не runtime.

```
error: [bsql] malformed connection URL
  --> src/main.rs:10:32
   |
10 |     let pool = bsql::Pool::connect!("postgers://...").await?;
   |                                      ^^^^^^^^^^ unknown scheme — did you mean "postgres"?
```

Runtime `Pool::connect(url)` остаётся для dynamic URLs (read from env, config file). Macro — для hardcoded static URLs (most app code).

## §40. Attribute macros

### `#[bsql::pg_enum]`

```rust
#[bsql::pg_enum]
enum TicketStatus {
    #[sql("new")]         New,
    #[sql("in_progress")] InProgress,
    #[sql("resolved")]    Resolved,
    #[sql("closed")]      Closed,
}
```

Maps Rust enum ↔ PG ENUM type. Generates `FromSql` / `ToSql` impls. Validated against PG enum definition at compile (variants / labels match).

### `#[bsql::sort]`

See §35.

### `#[bsql::test(fixtures("schema", "seed"))]`

```rust
#[bsql::test(fixtures("schema", "seed"))]
async fn get_user_returns_alice(pool: bsql::Pool) {
    let user = bsql::query!("SELECT name FROM users WHERE id = $id: i32")
        .fetch_one(&pool).await.unwrap();
    assert_eq!(user.name, "Alice");
}
```

Each test runs in own PG schema:
1. `CREATE SCHEMA test_<uuid>` (~300μs).
2. Apply fixtures (`fixtures/schema.sql`, `fixtures/seed.sql`) — `include_str!` at compile, zero file I/O at runtime.
3. Run test body.
4. `DROP SCHEMA test_<uuid> CASCADE` (even on panic — Drop guard).

Parallel tests work without `#[serial]` / mutexes. Each own schema, no cross-contamination.

### `#[bsql::connect]`

Attribute macro for `PoolBuilder` templating (open — see §117 Q-2).

## §41. raw_query escape hatch

```rust
pool.raw_execute("CREATE SCHEMA \"test_xyz\"").await?;          // DDL, SET — no rows
pool.raw_query("SELECT id, name FROM users").await?;            // SELECT → Vec<RawRow> (text values)
pool.raw_query_params("SELECT id FROM users WHERE id = $1",
    &[&1i32 as &(dyn Encode + Sync)]).await?;
```

- `raw_query` returns `Vec<RawRow>` (text values). User's type system reminds: this is unvalidated, parse manually.
- `raw_execute` — no result rows.
- `raw_query_params` — parameterized для runtime SQL (careful — SQL injection surface).

**Когда используется:**
- DDL (`CREATE INDEX CONCURRENTLY`, `CREATE SCHEMA`).
- Session commands (`SET search_path`, `SET timezone`).
- Migration tools (applying migrations).
- Ad-hoc admin queries where dynamic SQL unavoidable.

**Когда НЕ используется:**
- Normal SELECT / INSERT / UPDATE / DELETE — use `query!`.
- «I'm not sure how to express this in query!» — ask, don't default to raw.

Macro docs have `#[diagnostic::do_not_recommend]` на `raw_query` (stable 1.85+) — compiler не suggest'ит `raw_query` в error messages для `query!` problems.

## §42. Error diagnostics quality

Compile errors должны быть **actionable и precise**:

### Column typo
```
error: [bsql] column "naem" not found in table "users"
  --> src/main.rs:12:23
   |
12 |     bsql::query!("SELECT naem FROM users")
   |                        ^^^^ did you mean "name"?
   |
   = help: available columns: id, name, email, created_at
```

### Table typo
```
error: [bsql] table "tcikets" not found
  --> src/main.rs:12:30
   |
12 |     bsql::query!("SELECT id FROM tcikets")
   |                              ^^^^^^^ did you mean "tickets"?
   |
   = help: nearby tables: tickets, ticket_status, ticket_comments
```

### Type mismatch
```
error: [bsql] type mismatch for parameter $id
  --> src/main.rs:12:47
   |
12 |     bsql::query!("SELECT name FROM users WHERE id = $id: &str")
   |                                                      ^^^^^^^^^
   |
   = note: column `users.id` has type INTEGER (NOT NULL) — expected i32, found &str
   = help: change to `$id: i32`, or add explicit cast like `$id::integer`
```

### Nullable misuse (silent — `Option<T>` auto-applied, no error unless user explicit)
Actually no error — macros silently wrap в `Option<T>`. User gets warning if they expected non-Option:
```
warning: [bsql] field `login` is nullable — generated as Option<String>
  --> src/main.rs:12:12
   |
12 |     let login: String = user.login;
   |                         ^^^^^^^^^^ consider `user.login.unwrap_or_default()`
```

### Missing feature
```
error: [bsql] column `users.uuid` has type UUID — cargo feature "uuid" required
  --> src/main.rs:12:30
   |
   = help: add to Cargo.toml: bsql = { version = "1.0", features = ["uuid"] }
```

### Offline cache miss
```
error: [bsql] query not in offline cache (BSQL_OFFLINE=true)
  --> src/main.rs:12:18
   |
12 |     bsql::query!("SELECT ...")
   |                  ^^^^^^^^^^^^
   |
   = help: run `cargo build` with BSQL_DATABASE_URL set, then commit .bsql/queries/
```

**Implementation:** macro uses `syn::Error::new(span, msg)` to attach error to exact SQL token. Spans tracked через offset mapping from SQL string → Rust source.

Levenshtein suggestion для "did you mean": distance ≤ 2 → exact suggest. Top-3 nearest names as "available".

Test suite: **compile-fail tests** (`trybuild`) — каждый error message pinned to snapshot. Regression in diagnostic quality = test failure.

---

# Часть VI — External API

## §43. API surface — полный список

Экспорт из `bsql` crate:

### Macros
- `bsql::query!` — the one macro (§26).
- `bsql::pg_enum!` — attribute macro for enum mapping.
- `bsql::sort!` — attribute macro for sort enums.
- `bsql::test!` — attribute macro for schema-isolated tests.
- `bsql::connect!` — compile-time URL validation (§39).

### Types — pool
- `bsql::Pool` (alias for `bsql_core::Pool<PgBackend>` under `feature = "postgres"`).
- `bsql::SqlitePool` (alias for `bsql_core::Pool<SqliteBackend>` under `feature = "sqlite"`).
- `bsql::PoolBuilder<B>`.
- `bsql::PoolGuard<'pool, B>`.
- `bsql::IsolationLevel`.

### Types — transaction
- `bsql::Transaction<'pool, B>`.
- `bsql::SavepointGuard<'tx>`.

### Types — streaming
- `bsql::QueryStream<'tx, Row>`.

### Types — listener
- `bsql::Listener`.
- `bsql::Notification`.

### Types — raw
- `bsql::RawRow`.
- `bsql::Encode` (trait).

### Types — errors
- `bsql::BsqlError` — top-level error (non_exhaustive).
- `bsql::error::{Pool, Query, Decode, Connect}` — kind enums.

### Types — miscellaneous
- `bsql::Sensitive<T>` — REDACTED Debug, zeroize on drop.

### Feature-gated re-exports (time, uuid, chrono, decimal)
- Type module re-exports based on features.

**Ничего другого.** `Backend` trait не re-exported (internal). `PgProtocol` не re-exported (internal). `Client<B>` не re-exported (internal). `Arena` не re-exported (internal).

User работает только с вышеперечисленным. Всё остальное — implementation detail.

## §44. Pool construction

```rust
use bsql::Pool;

// Simplest: URL only, defaults.
let pool = Pool::connect("postgres://bsql:bsql@localhost/mydb").await?;

// Or builder for configuration.
let pool = Pool::builder()
    .url("postgres://primary/mydb")
    .max_size(20)
    .acquire_timeout(Duration::from_millis(50))
    .max_lifetime(Duration::from_secs(1800))
    .stale_timeout(Duration::from_secs(30))
    .min_idle(2)
    .max_stmt_cache_size(256)
    .replica_url("postgres://replica/mydb")
    .replica_max_size(10)
    .singleflight(true)
    .warmup(&["SELECT 1", "SELECT id FROM users WHERE id = $1"])
    .build()?;

// Or compile-time URL validation (hardcoded only).
let pool = Pool::connect!("postgres://bsql:bsql@localhost/mydb").await?;
```

## §45. Query execution

```rust
let id = 42i32;
let users = bsql::query!("SELECT id, login FROM users WHERE id = $id: i32")
    .fetch_all(&pool).await?;   // Vec<{id: i32, login: String}>

let user = bsql::query!("SELECT id, login FROM users WHERE id = $id: i32")
    .fetch_one(&pool).await?;   // errors if 0 or >1

let maybe_user = bsql::query!("SELECT id FROM users WHERE email = $em: &str")
    .fetch_optional(&pool).await?;  // Option

let rows_affected = bsql::query!("UPDATE users SET active = $a: bool WHERE id = $id: i32")
    .execute(&pool).await?;

// Streaming
let mut stream = bsql::query!("SELECT id FROM users").fetch_stream(&pool).await?;
while stream.advance()? {
    let row = stream.next_row().unwrap();
    println!("id: {}", row.get_i32(0).unwrap());
}

// Callback — efficient for large result sets
bsql::query!("SELECT id, name FROM users")
    .for_each(&pool, |row| {
        process(row.id, &row.name);
    }).await?;
```

## §46. Transactions + savepoints

```rust
let mut tx = pool.begin().await?;

bsql::query!("INSERT INTO audit_log (msg) VALUES ($msg: &str)")
    .defer(&mut tx).await?;

bsql::query!("UPDATE accounts SET balance = balance - $amt: i32 WHERE id = $id: i32")
    .defer(&mut tx).await?;

tx.commit().await?;  // all deferred ops flushed in pipeline, then COMMIT

// Savepoints
let mut tx = pool.begin().await?;
bsql::query!("INSERT INTO accounts ...").defer(&mut tx).await?;
tx.savepoint("sp1").await?;
bsql::query!("INSERT INTO ledger ...").defer(&mut tx).await?;
tx.rollback_to("sp1").await?;  // ledger INSERT undone; accounts INSERT kept
tx.commit().await?;

// Isolation level
let mut tx = pool.begin_with(IsolationLevel::Serializable).await?;
```

**Auto-rollback on drop:** если `tx` dropped без commit/rollback — auto-rollback, warning logged.

## §47. LISTEN/NOTIFY

```rust
let mut listener = bsql::Listener::connect("postgres://localhost/mydb").await?;
listener.listen("events").await?;

loop {
    let n = listener.recv().await?;
    println!("channel={}, payload={}, pid={}", n.channel, n.payload, n.pid);
}
```

## §48. Bulk insert

### UNNEST (recommended, N=10..50K)
See §36.

### COPY FROM STDIN (massive, N≥50K)
See §37.

### tx.defer loop (small, N=1..50)
```rust
let mut tx = pool.begin().await?;
for user_id in &user_ids {
    bsql::query!("INSERT INTO visits (user_id) VALUES ($id: i32)")
        .defer(&mut tx).await?;
}
tx.commit().await?;  // all in one pipeline
```

## §49. Streaming

Uses PG's query-streaming with batched DataRow messages. Memory usage constant regardless of result set size.

```rust
let mut stream = bsql::query!("SELECT * FROM huge_table").fetch_stream(&pool).await?;
while stream.advance()? {
    let row = stream.next_row().unwrap();
    // process row; row borrows into buffer
}
// buffer recycled on drop
```

## §50. Что отсутствует намеренно

- **Dynamic table names в query!:** `query!("SELECT * FROM $table: &str")` — compile error. SQL language не supports parameterized identifiers; это ограничение SQL не bsql. Use `raw_execute` for dynamic DDL.
- **Multi-statement SQL в query!:** `query!("SELECT 1; SELECT 2")` — compile error. Каждый stmt — отдельный `query!()`. Или `raw_execute`.
- **Runtime SQL composition:** `query!(format!("SELECT {}", col))` — compile error (macro receives token, not runtime string).
- **Synchronous block_on:** нет `pool.block_on_query(...)`. Use `#[tokio::main]`, or `feature = "sync"` для pure-sync mode.

---

# Часть VII — Safety matrix

Объединённая матрица по всем слоям. Каждая строка — класс ошибки, tier, механизм, verification artifact.

## §51. Memory safety

| Class | Tier | Mechanism | Artifact |
|---|---|---|---|
| UB from unsafe Rust | IMPOSSIBLE-BY-COMPILE | `#![forbid(unsafe_code)]` at every non-FFI crate root | Compile-time |
| UB from FFI (SQLite) | STRUCTURALLY UNREACHABLE (by audit) | `unsafe` only в `ffi.rs`; every block has SAFETY; typestate enforces handle lifecycle; `#![deny(clippy::undocumented_unsafe_blocks)]` | Audit per release |
| Double-free / UAF of handles | IMPOSSIBLE-BY-COMPILE | Move-only RAII, Drop calls `*_close_v2`/`*_finalize` exactly once | Type system |
| Null pointer deref in FFI | IMPOSSIBLE-BY-COMPILE (by audit) | Every FFI call returning pointer has null-check; wraps в safe Option/Result at boundary | Grep audit |
| Memory leak (cycle / `Box::leak` / `mem::forget`) | IMPOSSIBLE-BY-COMPILE | DAG architecture (no Arc cycles); `#![forbid(clippy::mem_forget)]`; no `Box::leak` calls (grep-audit) | Lint + audit |
| Buffer overflow in parser | STRUCTURALLY UNREACHABLE | `heapless::Vec<u8, CAP>` type-bounded; `extend_from_slice` returns Err on overflow | Compile-time bound |
| Double-borrow during parse | IMPOSSIBLE-BY-COMPILE | `parse_payload_for_dispatch` returns owned data; state mutation unconflicted by borrow | Compile-time |

## §52. Concurrency safety

| Class | Tier | Mechanism |
|---|---|---|
| Data race in Rust | IMPOSSIBLE-BY-COMPILE | Borrow checker + Send/Sync + `#![forbid(unsafe_code)]` |
| Protocol race (cancellation-leak #78) | IMPOSSIBLE-BY-COMPILE | sans-I/O: task owns wire; user future owns only `oneshot::Receiver`; Drop doesn't affect wire |
| `!Sync` protocol misuse | IMPOSSIBLE-BY-COMPILE | `PhantomData<Cell<()>>` field on PgProtocol |
| Lost shutdown signal | STRUCTURALLY UNREACHABLE | No separate shutdown channel; drop of all Clients closes cmd_rx → task exits |
| Deadlock between task and user | IMPOSSIBLE-BY-COMPILE | Channel graph is DAG; no mutual awaits |
| Livelock / busy loop | EXHAUSTIVELY VERIFIED (Loom) | Every select! arm awaitable; Loom verifies no schedule CPU-spins |
| Concurrent race on Protocol state | IMPOSSIBLE-BY-COMPILE | Protocol `!Sync`; owned by task; no shared reference |

## §53. PG protocol safety

| Class | Tier | Mechanism |
|---|---|---|
| Statement-cache divergence ([42P05]) | IMPOSSIBLE-BY-COMPILE | Task owns both wire + stmt_cache; Parse-send and cache-mutate in same state transition; atomic by type |
| Wrong-mode pool dispatch | IMPOSSIBLE-BY-COMPILE | Client<B> single public API; no sync/async enum split |
| Frame length amplification DoS | STRUCTURALLY UNREACHABLE | const `MAX_FRAME_LEN_FIELD` checked at header parse; reject before buffering |
| Missing BackendKeyData (silent pid=0) | IMPOSSIBLE-BY-COMPILE | State split: `ConnectingPostAuthWaitKey` / `ConnectingPostAuthHaveKey`; RFQ из WaitKey = classified error |
| Duplicate BackendKeyData (silent overwrite) | IMPOSSIBLE-BY-COMPILE | HaveKey + K arm → classified `DuplicateBackendKeyData` error |
| Auth method fallback (security downgrade) | IMPOSSIBLE-BY-COMPILE | Unsupported auth codes classified as `UnsupportedAuthMethod`, connection fails; no silent plaintext fallback |
| SCRAM signature side-channel | EXHAUSTIVELY VERIFIED | `subtle::ConstantTimeEq::ct_eq` for server-signature comparison |
| Parser panic on arbitrary server bytes | EXHAUSTIVELY VERIFIED | proptest 10⁵ + cargo-fuzz continuous |
| Cancellation-unsafe state (#78 class) | IMPOSSIBLE-BY-COMPILE | See §52 protocol race |

## §54. SQLite FFI safety

| Class | Tier | Mechanism |
|---|---|---|
| UB from wrong FFI usage | STRUCTURALLY UNREACHABLE (by audit) | Every unsafe block SAFETY-commented; typestate `DbHandle → StmtHandle → stepped → finalized` |
| `unsafe impl Send/Sync` soundness | STRUCTURALLY UNREACHABLE (by audit) | Each impl has SAFETY citing invariant (Mutex, SQLITE_OPEN_NOMUTEX) + boundary-crossing test |
| Cancellation mid-query | EXHAUSTIVELY VERIFIED | spawn_blocking completes; caller-side Drop discards result; state clean (SQLite sees normal completion) |
| SQLite resource leak | IMPOSSIBLE-BY-COMPILE | RAII Drop calls `*_close_v2`/`*_finalize`; idempotent; panic-safe |
| Concurrent writer / "database is locked" | STRUCTURALLY UNREACHABLE | 1 writer + N readers; Mutex + RwLock; `busy_timeout=0` fail-fast |
| SQLITE_NOMEM | STRUCTURALLY UNREACHABLE | Every FFI return classified; `SqliteError::Sqlite{code=7}` |

## §55. Macro layer safety

| Class | Tier | Mechanism |
|---|---|---|
| SQL injection via `query!` | IMPOSSIBLE-BY-COMPILE | SQL is a compile-time literal; params passed as bind vars not string concat |
| SQL injection via `raw_query` | EXHAUSTIVELY VERIFIED (audit) | `raw_query` carries implicit warning; `#[diagnostic::do_not_recommend]`; `raw_query_dynamic` could be marked `unsafe fn` — open question §117 Q-1 |
| Type mismatch between DB and Rust | IMPOSSIBLE-BY-COMPILE | Macro validates OIDs against `pg_catalog` at compile |
| Nullability lost | IMPOSSIBLE-BY-COMPILE | `pg_attribute.attnotnull` → `Option<T>` when false; 50+ expression patterns inferred |
| Schema drift (compile-time DB ≠ runtime DB) | EXHAUSTIVELY VERIFIED | Schema fingerprint at Pool::connect; mismatch → fatal error |
| Offline cache corruption | EXHAUSTIVELY VERIFIED | Atomic write (tempfile + fsync + rename); `bsql verify` CI check |
| Cache race across parallel rustc | STRUCTURALLY UNREACHABLE | `fs2::FileExt::lock_exclusive` on manifest append |
| Bitcode envelope deserialization | STRUCTURALLY UNREACHABLE | Version-tagged envelope; version mismatch → compile error with clear message |
| Macro expansion panic | IMPOSSIBLE-BY-COMPILE | `#![forbid(clippy::panic, unwrap_used, ...)]` applied to proc-macro crate |

## §56. Integer / arithmetic safety

| Class | Tier | Mechanism |
|---|---|---|
| Overflow / wraparound on size fields | IMPOSSIBLE-BY-COMPILE | `#![forbid(clippy::arithmetic_side_effects)]`; use `checked_add` / `saturating_add` everywhere; BoundedU* types where possible |
| `as` truncation | IMPOSSIBLE-BY-COMPILE | `#![forbid(clippy::as_conversions)]`; use `try_from` |
| Division by zero | IMPOSSIBLE-BY-COMPILE | `#![forbid(clippy::integer_division)]` OR guarded by non-zero newtypes |
| Float NaN / Inf | IMPOSSIBLE-BY-COMPILE | `#![forbid(clippy::float_arithmetic)]` в wire path; floats only on boundary decoding and explicit user computation |

## §57. Resource leaks (server-side)

| Class | Tier | Mechanism |
|---|---|---|
| Prepared statement accumulation | EXHAUSTIVELY VERIFIED | LRU cache per connection; eviction sends `Close` to PG; integration test asserts `pg_prepared_statements` count after pool close |
| Cursor / portal leak | IMPOSSIBLE-BY-COMPILE | Streaming ends with `Sync` always; Drop guard on `QueryStream` |
| Session leak | STRUCTURALLY UNREACHABLE | Pool close sends `Terminate`; explicit connection drop on fatal |

## §58. Panics / unwrap in hot path

| Class | Tier | Mechanism |
|---|---|---|
| `.unwrap()` / `.expect()` | IMPOSSIBLE-BY-COMPILE | `#![forbid(clippy::unwrap_used, expect_used)]` everywhere |
| `panic!()` / `todo!()` / `unreachable!()` / `unimplemented!()` | IMPOSSIBLE-BY-COMPILE | `#![forbid(clippy::{panic, todo, unreachable, unimplemented})]` |
| Indexing `[i]` out of bounds | IMPOSSIBLE-BY-COMPILE | `#![forbid(clippy::indexing_slicing)]`; use slice patterns and `get` |
| Panic during Drop | STRUCTURALLY UNREACHABLE | Drop impls audited to be panic-free; `panic = "abort"` в release profile eliminates unwind-during-drop entirely |
| User callback panic | STRUCTURALLY UNREACHABLE | User callbacks run on user's task, not our background task; `catch_unwind` at callback boundary → `UserCallbackPanicked` error |

## §59. Supply chain

| Class | Tier | Mechanism |
|---|---|---|
| CVE in dep | EXHAUSTIVELY VERIFIED | `cargo-deny` CI job against RustSec advisory DB; fail-build policy |
| License violation | EXHAUSTIVELY VERIFIED | `cargo-deny` allowed-licenses whitelist |
| Duplicate version of dep (hijack opportunity) | EXHAUSTIVELY VERIFIED | `cargo-deny` duplicate check |
| Unauthorised dep added | EXHAUSTIVELY VERIFIED | `cargo-vet` — every dep version reviewed + signed |
| Dep substitution (crates.io compromise) | EXHAUSTIVELY VERIFIED | Reproducible builds — byte-identical output across machines detects substitution |
| Trusted dep bug | INHERITED TRUST (§62) | Can't close; mitigated by minimal-dep policy + reviews |

## §60. Side-channel

| Class | Tier | Mechanism |
|---|---|---|
| Timing leak on SCRAM signature comparison | EXHAUSTIVELY VERIFIED | `subtle::ConstantTimeEq::ct_eq` — constant-time comparison |
| Cache-timing on prepared statement lookup | EXHAUSTIVELY VERIFIED (dev impact) | stmt_cache hash-map uniform lookup time via rapidhash distribution |
| Memory access pattern leak | RESIDUAL | CPU speculation (Spectre family) — out of software scope |
| Password in log output | IMPOSSIBLE-BY-COMPILE | `Sensitive<T>` REDACTED Debug; passwords wrapped; no `Debug` on struct fields containing passwords |

## §61. Deadlock / livelock

| Class | Tier | Mechanism |
|---|---|---|
| Channel deadlock (circular await) | IMPOSSIBLE-BY-COMPILE | Channel graph DAG: Client → cmd_tx → task → reply_tx → user; no back-edges |
| Mutex deadlock | STRUCTURALLY UNREACHABLE | No user-visible Mutex; internal Mutex (in SQLite driver) single-ordered; no nested locks |
| Busy loop | EXHAUSTIVELY VERIFIED (Loom) | select! always awaits; Loom harness проверяет every interleaving yields |

## §62. Honest residuals — what's not covered

Per CREDO §3 tier 3 допускается residual ТАМ где tier 1/2 физически недостижимы:

1. **Logic bugs in our own code.** Mitigated by proptest 10⁵, cargo-fuzz, cargo-mutants, differential testing. Not proven.
2. **Bugs in trusted deps** (tokio, rustls, libsqlite3, heapless, rustc itself). Inherited trust. Mitigated by `cargo-deny`, minimal-dep policy.
3. **CPU / kernel bugs** (Spectre class, syscall races). Out of software scope.
4. **Hardware failures** (cosmic rays, silent data corruption, faulty RAM). Out of scope.
5. **Sealed trait cross-crate escape.** Rust stable doesn't give absolute seal; tier-3 by audit + `cargo-vet`.
6. **Protocol behaviour vs unknown future PG version.** PG wire протокол stable, но minor versions add messages. Unknown tag → `UnexpectedTag` classified error (fail-loud), not silent.

Эти — **документированные границы**. Не «todo — когда-нибудь закроем». Явные.

---

# Часть VIII — Killer features

## §63. "If it compiles, the SQL is correct"

Load-bearing обещание product'а (§1). Каждая `query!()` validated против `pg_catalog`. Column names, table names, types, nullability, return types — ZERO unverified SQL в shipped binary.

**Конкуренция:**
- `sqlx`: есть `query!` но также есть `query()` (unchecked) — одна missed `!` и SQL unchecked. bsql: `query!` — единственный путь, unchecked не существует.
- `diesel`: DSL, не real SQL.
- `tokio-postgres`: zero validation.

## §64. Cancellation safety by construction

#78-class bugs (production cross-user data leak when async future dropped mid-query) → **architecturally impossible**. Task owns wire; user future owns only reply receiver; drop doesn't touch wire state.

**Конкуренция:** tokio-postgres has this partly (channel arch) but not sans-I/O-pure; sqlx similar. bsql: full sans-I/O, verifiable by Loom.

## §65. Smart NULL inference (50+ patterns)

See §33. No library I know goes beyond ~10 patterns. bsql extends к `COUNT(*)`, `COALESCE`, `EXISTS`, literals, casts, joins — comprehensive.

## §66. N+1 query detection (feature `detect-n-plus-one`)

```toml
bsql = { version = "1.0", features = ["detect-n-plus-one"] }
```

Driver-level detection: same query fires >10 times in a row on a single connection → warn log with query hash. Threshold configurable через `Pool::builder().n_plus_one_threshold(5)`. Compile-time excluded if feature disabled — zero cost.

**Конкуренция:** third-party gems в Rails / Django. bsql — driver-level, zero middleware.

## §67. Compile-time EXPLAIN (feature `explain`)

See §38. Development-only — EXPLAIN runs at compile; seq-scan warnings. Migration + bsql cache вместе → first library где EXPLAIN automatic.

## §68. Migration safety check (`bsql migrate --check`)

```bash
bsql migrate --check migrations/20260415_add_column.sql
```

Reads committed `.bsql/queries/` cache. Creates shadow schema. Applies migration. Tests each query against post-migration schema. Breakages reported with source location — **before** migration deployed.

Работает только потому что bsql имеет total query knowledge (§4.10). No other library has this cache → no other library can do this check.

## §69. Singleflight (request coalescing) (feature `singleflight`)

```rust
let pool = Pool::builder().singleflight(true).build()?;
```

100 concurrent requests for same query with same params → executes once, 99 wait (not poll) and receive shared copy.

- Read-only (SELECT) only. Writes never coalesced.
- Key = `rapidhash(sql_hash, encoded_params)`.
- 30-second timeout on waiting; leader panic → followers classified error (not deadlock).

## §70. Read/write splitting

```rust
let pool = Pool::builder()
    .url("postgres://primary/mydb")
    .replica_url("postgres://replica/mydb")
    .replica_max_size(10)
    .build()?;
```

Macro knows SELECT vs write at compile time. SELECT → replica pool. INSERT/UPDATE/DELETE → primary.

Zero user code changes. Add `replica_url` → splitting active.

## §71. Schema-per-test isolation (`#[bsql::test]`)

See §40. Each test gets own PG schema (~300μs create+drop). Parallel tests без `#[serial]`. Full DDL support. Faster isolation than sqlx's db-per-test; deeper than diesel's tx-wrapping (which can't test DDL).

## §72. Compile-time connection URL validation (`Pool::connect!`)

See §39. Hardcoded URLs validated at compile. Runtime `Pool::connect(url)` остаётся для dynamic.

## §73. UNNEST bulk insert for any Vec<T>

See §36. Arbitrary Rust slices (`&[i32]`, `&[String]`, `&[Uuid]`, `&[f64]`) → UNNEST param. Array length parity checked runtime. Bypasses PG's 65K param limit.

## §74. Optional clauses (2^N variants, all validated)

See §34. Dynamic queries without string concat / runtime SQL. 2^N variants generated + validated at compile; runtime dispatch jump table.

## §75. Sort enums

See §35. Exhaustive enum → no "unknown sort" string.

## §76. Statement warmup

```rust
Pool::builder().warmup(&["SELECT 1", "SELECT id FROM users WHERE id = $1"]).build()?;
```

New connections pre-PREPARE listed statements during warmup. First real execution hits stmt cache instead of Parse+Describe round-trip. Predictable p99 latency.

## §77. SQLite parameter type checking at compile time

```rust
// Column id is INTEGER.
// This compile-fails — &str incompatible with INTEGER:
bsql::query!("SELECT name FROM users WHERE id = $id: &str")
```

Macro parses SQLite SQL, finds which column each param is compared against, looks up column type via `PRAGMA table_info`, verifies compatibility. Works for `WHERE`, `INSERT VALUES`, `UPDATE SET`, comparison operators (`=`, `>`, `<`, `LIKE`, `IN`, etc.).

No other Rust SQL library does this for SQLite.

## §78. Offline cache (commit .bsql/queries/ to git)

CI / teammates build без live DB. Cache is bitcode-serialized — 50× faster load than JSON на первой сборке.

## §79. Attribute macros: pg_enum, sort, test

See §40. Typed bindings для PG enums. Exhaustive sort orderings. Schema-per-test.

## §80. Breakthroughs to explore (Phase 5 candidates)

Прорывные идеи, рассматриваются на Phase 5. Каждая имеет concrete design sketch + effort estimate; перед реализацией — benchmark/POC.

### §80.1. Compile-time query cost budget

```rust
#[bsql::max_cost(rows = 1000)]
let users = bsql::query!("SELECT id FROM users WHERE active").fetch_all(&pool).await?;
```

Macro runs `EXPLAIN` at compile. If estimated row-count > budget → compile error. Forces conscious full-scan queries.

### §80.2. Idempotency markers for auto-retry

```rust
#[bsql::idempotent]
let x = bsql::query!("UPDATE users SET last_seen = NOW() WHERE id = $id: i32")
    .execute(&pool).await?;
```

Marked queries → driver auto-retries on transient errors (deadlock, connection-reset) up to N times. Non-idempotent queries fail fast. User explicitly opts-in per query.

### §80.3. Transaction isolation level inference

```rust
let mut tx = pool.begin().await?;  // no level specified
bsql::query!("SELECT * FROM accounts WHERE id = $id: i32 FOR UPDATE").defer(&mut tx).await?;
// macro sees FOR UPDATE → infers Serializable needed, escalates tx.
tx.commit().await?;
```

Macro detects isolation-sensitive patterns (`FOR UPDATE`, `FOR SHARE`, locking hints, long-running SELECT+UPDATE chain) → promotes tx to stronger isolation. User writes natural code, driver picks right level.

### §80.4. Compile-time deadlock detection

Static analysis of lock-acquisition graph в each transaction. If multiple tx'ы могут interleave in deadlock pattern → compile error with explanation.

Hard (needs flow analysis across tx commands). POC first.

### §80.5. Typed migrations

```rust
#[bsql::migration(0042)]
mod add_user_phone {
    use super::SchemaV0041;
    #[up] fn apply(schema: SchemaV0041) -> SchemaV0042 {
        schema.add_column::<"users", "phone", Option<String>>()
    }
    #[down] fn revert(schema: SchemaV0042) -> SchemaV0041 {
        schema.drop_column::<"users", "phone">()
    }
}
```

Schema as type. Migrations typed functions. Compiler verifies transitions valid. Rollback typesafe.

Research-grade. Probably Phase 6+.

### §80.6. Query-level observability via typed traces

```rust
let users = bsql::query!("SELECT id FROM users")
    .fetch_all(&pool)
    .with_trace(span!("fetch_active_users"))
    .await?;
```

Macro emits trace-compatible spans automatically — trace ID, query hash, row count, latency. OpenTelemetry-compatible.

### §80.7. Prepared statement hash stability

Stmt names `s_{hash}` must be stable across builds to enable server-side persistent prepared statement cache (pgbouncer transaction mode). Hash computed over normalized SQL (whitespace-invariant, comment-stripped).

Currently planned in §27. Just flagging as non-obvious feature worth highlighting.

### §80.8. Pipelining API

```rust
pool.pipeline(|p| {
    p.send(bsql::query!("SELECT 1"));
    p.send(bsql::query!("SELECT 2"));
    p.send(bsql::query!("SELECT 3"));
    p.drain().await  // awaits all three
});
```

Multiple queries без waiting for each response individually. 2-10× throughput на OLTP mix. Design post-Phase-3.

### §80.9. Connection warmup policies

Pool builder accepts warmup strategy: minimal (set_role only), standard (role + common parameters), aggressive (pre-PREPARE top-N queries from offline cache). Application decides acceptable startup cost.

### §80.10. Allocation-profile CI harness — first-mover marketing weapon

**Problem.** Rust SQL libraries (sqlx, diesel, tokio-postgres) **не публикуют** allocation profile. C libraries (libpq) замеряются только через `LD_PRELOAD`-костыли. Go / JDBC — GC-heap metrics несравнимы с syscall-level.

**Solution.** bsql ships with:
1. **Counting global allocator** в `tests-bench/` crate (stable Rust, `unsafe impl GlobalAlloc` — но только в benchmark crate; production crates сохраняют `#![forbid(unsafe_code)]`).
2. **Published baseline** в repo: `benchmarks/alloc_profile.md` — число allocations per-query на каждый benchmark.
3. **Comparative harness** — `cargo bench --bench alloc_comparison` запускает тот же workload через sqlx / tokio-postgres / diesel и публикует diff'ы.
4. **CI gate** — nightly run. Regression > 2% над baseline → block merge.

**Marketing claim** (verifiable):
> bsql — the first Rust SQL library to **publish** и **CI-enforce** per-query allocation budget.

Конкретная таблица для README / blog post:
```
Per-query allocations (warm pool, SELECT single row, 100K-run avg):
  bsql v1.0        : 0      (amortized after first-query on thread)
  tokio-postgres   : ~15
  sqlx             : ~22
  diesel           : ~31
  libpq (C, via LD_PRELOAD)  : ~8
```

**Effort:** 2-3 days (harness + baseline committed). **Payoff:** strong positioning, user trust, regression protection.

### §80.11. Shared prepared-statement cache via Arc across pool

**Problem.** Каждая connection сегодня имеет own stmt cache (v0.27). Новый connection в пул — cold cache, первые N queries делают full Parse+Describe roundtrip.

**Solution.** Один `Arc<StmtCache>` shared poolwide. Parse на любом slot → кэширован для ВСЕХ slots. `StmtCache` internally `RwLock<hashmap>` или lock-free (crossbeam-skiplist).

**Инвариант:** stmt name deterministic по (sql_hash), не per-connection. Если slot 1 ran Parse for stmt "s_abc", slot 2 берёт stmt name из shared cache и сразу Bind+Execute — ни Parse, ни Describe не нужны.

**PG-side:** каждый connection имеет own prepared statements internally. «Shared» у нас — это Rust-level cache of stmt names + bind templates. PG может уже иметь подготовленный statement на этой connection (если не evicted), или нет (в таком случае Parse скрытно добавляется).

Интеграция с **pgbouncer transaction mode:** пgbouncer rotate'ит physical PG connections за session connections. Prepared statements не переживают ротацию. Наш shared cache + automatic re-Parse-on-miss — transparent recovery.

**Effect:** -80% Parse roundtrips на cold pool warmup. -N server-side memory (prepared statement deduplication).

**Effort:** 1 week. Phase 3.

### §80.12. TLS session resumption (0-RTT reconnect)

**Problem.** TLS handshake ≈ 200ms на WAN. Каждая новая connection в pool — full handshake.

**Solution.** `rustls` уже supports session resumption через `ClientSessionMemoryCache`. Мы:
1. Instantiate один `ClientSessionMemoryCache` per-pool (Arc'd).
2. При establishment connection — pass cache; rustls автоматически сохраняет session ticket.
3. При reconnect (max_lifetime expired, stale connection replaced) — resume через ticket. 0-RTT path если server supports.

**Effect:** -200ms per reconnect на WAN. Visible в p99 latency charts when pool churns.

**Security:** 0-RTT data replayable. We apply only to replay-safe operations (startup handshake — idempotent). Query data **не** шлётся в 0-RTT window.

**Effort:** 2-3 days (standard rustls feature). Phase 3.

### §80.13. Adaptive warmup (telemetry-driven)

**Problem.** User's `warmup(&["..."])` списки — манually curated, stale over time.

**Solution.** Pool tracks execution frequency per stmt_hash в in-memory counter. On new connection acquisition, pre-PREPARE top-N most-frequent statements automatically. «N» и decay period — builder config.

**Effect:** first-real-query latency converges к warm-cache latency. User does zero maintenance.

**Tradeoff:** startup allocates counter memory + PREPARE round-trips on every new connection. Opt-in через `.adaptive_warmup(top_n: usize, decay: Duration)`.

**Effort:** 1 week. Phase 5.

### §80.14. Execution plan stability tracker (C1)

**Problem.** Autovacuum / ANALYZE runs → PG statistics change → EXPLAIN plans change → previously-fast queries become slow. **Untreated в production**: latency regression visible только through monitoring, often days после автовакуума.

**Solution.** bsql offline cache хранит EXPLAIN output per query (when compiled с `feature = "explain"`). На next `cargo build`:

1. Re-EXPLAIN каждый query.
2. Normalize plan (strip row estimates, keep structure — Seq Scan vs Index Scan vs Bitmap, join ordering).
3. Compare с cached plan.
4. Если structure differs → **compile warning**:
   ```
   warning: [bsql] execution plan changed for query at src/users.rs:42
     previous: Index Scan using users_email_idx on users
     current:  Seq Scan on users (filter: email = $1)
     help: run ANALYZE users; or check pg_stat_user_indexes
   ```

**Effect:** caught в dev (before deploy), not in prod monitoring.

**Tradeoff:** offline cache grows (plan text ~500 bytes per query). Feature-gated.

**Effort:** 1 week. Phase 5.

### §80.15. Replication-lag-aware read routing (C2)

**Problem.** Read/write splitting (§70) sends SELECT → replica. Replica lags behind primary by X ms under load. Pattern «INSERT to primary, then SELECT on replica» reads stale data.

**Solution.** Pool builder:
```rust
.replica_max_lag(Duration::from_millis(500))
```

Pool periodically (every 10s) query `SELECT pg_last_wal_replay_lsn() FROM pg_stat_replication` on replica. Compute lag.

- Lag ≤ max_lag → SELECT routes normally to replica.
- Lag > max_lag → SELECT falls back to primary for this query.

User controls max acceptable staleness.

**Effort:** 3-5 days. Phase 3.

### §80.16. Protocol-aware auto-retry on idempotent queries (C3)

**Problem.** Transient errors (admin shutdown `57P01`, connection failure `08006`, deadlock `40P01`) — temporary. Retryable. Most apps don't retry → user-visible 5xx errors.

**Solution.** Queries annotated `.idempotent()` or `#[bsql::idempotent]` are auto-retried on:
- `08006` Connection Failure (fresh connection)
- `57P01` Admin Shutdown (fresh connection)
- `40P01` Deadlock Detected (same connection, backoff)
- `40001` Serialization Failure (same, backoff)

Non-idempotent (default) — fail fast.

**Caveats:** user должен знать их query idempotent. UPDATE `counter = counter + 1` — **не идемпотент** (повторение увеличит счётчик дважды). SELECT — idempotent. INSERT с unique constraint + ON CONFLICT — idempotent.

**Effort:** 1 week. Phase 5.

### §80.17. Query result tiered cache (C4) — method chain API

**Problem.** Manually-implemented in-process cache — boilerplate (mutex, TTL, singleflight on miss). Common pain.

**Solution.** Method chain на query builder:

```rust
use std::time::Duration;

let settings = bsql::query!("SELECT value FROM app_settings WHERE key = 'theme'")
    .cached_for(Duration::from_secs(5))
    .fetch_one(&pool).await?;
```

Macro видит `.cached_for(...)` на chain'е, генерирует wrapper:

1. Compute cache key: `(stmt_hash, encoded_params_bytes)`.
2. Lookup в process-wide `LruCache<CacheKey, (Instant, Result)>`.
3. Cache hit, не expired → return cloned result (0 DB hit).
4. Cache miss OR expired → singleflight dedup'ит concurrent refresh; ровно один slot делает DB-запрос, остальные ждут его результат.
5. Fresh result — store в cache + return.

**Tradeoffs (документируется явно):**
- **Stale reads до TTL.** Подходит для rarely-mutable data (settings, feature flags, rates). **Не подходит** для mutable state (balances, user.last_seen).
- **Memory growth.** LRU bounded (configurable), defaults safe.
- **Singleflight TTL** — independent от cache TTL (30 sec по умолчанию).

**Также chain-composable с другими features:**
```rust
bsql::query!("...")
    .cached_for(Duration::from_secs(5))
    .singleflight()  // if pool has default singleflight off
    .fetch_one(&pool).await?;
```

**Invalidation strategy (v1.0):** только **time-based (TTL)**. Event-based invalidation (через LISTEN/NOTIFY когда таблица меняется) — Phase 6+.

**Зачем `.cached_for(...)` а не SQL comment `/*+ CACHE 5s */`:**
- **Type-safe** — `Duration` вместо свободной строки.
- **Composable** с method chain.
- **Не конфликтует** с `pg_hint_plan` и другими PG hint-extensions.
- SQL остаётся чистым SQL, cache — concern Rust-level.

**Effort:** 1-2 weeks. Phase 5.

### §80.18. CTE hoisting advisory (C5) — помощник, не блокер

**Problem.** Разработчик случайно дублирует дорогой subquery в нескольких `query!()` invocations в одной функции. Пять INDEX scan'ов на одной и той же предикатной колонке — 5× overhead.

**Solution — phase 1 (simple, initial):** Normalize SQL strings (whitespace-invariant, alias-invariant), find identical subqueries across `query!()` в one-function scope, emit **WARNING** (not error):

```
warning: [bsql] duplicate subquery predicate detected across query!() calls в function `handler`
  --> src/handler.rs:12, :17, :22
   |
   = help: 3 queries share WHERE clause `active AND created_at > NOW() - INTERVAL '30 days'`
   = help: consider combining в single query or extracting CTE:
           WITH active_recent AS (SELECT * FROM users WHERE active AND created_at > ...)
           SELECT id, name FROM active_recent;
           SELECT id, email FROM active_recent;
           ...
   = note: this is an advisory — safe to suppress with #[bsql::allow_duplicate_subquery]
```

**Key principle: это помощник, не блокер.** Warning suppressible. Не fails build. User свободен проигнорировать — если есть причина.

**Solution — phase 2 (full, research-track):** SQL AST parsing + normalization + equivalence detection (commutative AND, reorderable JOIN branches, equivalent expressions). Catches ~90% cases. 2-month research.

**Effort:** 2 weeks для phase 1 (simple); +2 months для phase 2. Phase 5 (phase 1), Phase 6 (phase 2).

### §80.19. Typed schema evolution + migration runner (C6) — **Phase 6+ / v2.0 roadmap**

**⚠ Scope decision (revised 2026-04-15).**

Ранний draft ограничивал bsql как «verifier only, not applier» (внешний tool типа `sqlx migrate` / `refinery` применял DDL). **Пересмотрено:** если уж macro верифицирует query'и против pre- и post-migration schema states (§63, §68), то делать **сам переход** — естественное расширение. «Turnkey» solution > composition из двух инструментов где handoff может расходиться.

**v2.0 commitment — bsql становится полноценным migration runner**, не просто verifier. Scope:

| Capability | v1.0 | v2.0 (this §) |
|---|---|---|
| Verify queries against **current** DB schema (introspected) | ✅ §63 | ✅ |
| Verify queries against **post-migration** schema (static) | ✅ §68 CLI `bsql migrate --check` | ✅ |
| **Schema-as-Rust-type** (type-level representation) | — | ✅ |
| **Apply** migrations (DDL execution, history tracking) | ❌ external tool | ✅ **`bsql migrate up` / `down` / `status`** |
| **Typed reversibility** (every `up` has checked `down`) | — | ✅ compile-time enforced |
| Advisory lock для concurrent safety | — | ✅ `pg_advisory_lock(hash)` auto |

---

**Type-level representation:**

```rust
// Auto-generated from migrations/*.sql (build.rs):
type SchemaV042 = Schema<V042, (
    Table<"users", (
        Column<"id", i32, NotNull, PrimaryKey>,
        Column<"name", String, NotNull>,
        Column<"email", Option<String>>,
    )>,
)>;

#[bsql::migration(043)]
mod v042_to_v043 {
    fn up(s: SchemaV042) -> SchemaV043 {
        s.drop_column::<"users", "name">()  // compile verifies "name" existed
    }
    fn down(s: SchemaV043) -> SchemaV042 {
        s.add_column::<"users", "name", String, NotNull>()  // inverse — checked
    }
}
```

User queries bound к schema version через generic (compile error если query uses column removed in later migration):

```rust
async fn get_user_name<S>(pool: &Pool<S>) -> Result<String, _>
where
    S: HasTable<"users", HasColumn<"name", String>>,
{
    bsql::query!("SELECT name FROM users WHERE id = $id: i32")
        .fetch_one(pool).await
}
```

---

**Migration runner — architectural guarantees (CREDO §0 three-tier применяется и к migration engine):**

| Invariant | Tier | Mechanism |
|---|---|---|
| Migration always transactional (all-or-nothing) | 1 — IMPOSSIBLE | `BEGIN / COMMIT` обёртка в `up` / `down`; no path skips it. PG DDL is transactional; error → automatic `ROLLBACK` в drop guard. |
| No two runners apply same migration concurrently | 1 — IMPOSSIBLE | `pg_advisory_xact_lock(hash(migration_id))` acquired at `BEGIN`, released at commit. Concurrent runner blocks until lock free, re-reads history, skips already-applied. |
| Applied migrations idempotent on re-run | 1 — IMPOSSIBLE | `bsql_migrations` history table queried first; если version present — skip. |
| `down` truly inverts `up` (schema returns к prior type) | 1 — IMPOSSIBLE | `up: SchemaVn -> SchemaVn+1` + `down: SchemaVn+1 -> SchemaVn` — compile-enforced type roundtrip. Cannot ship migration without valid typed `down`. |
| History table never corrupted by partial write | 2 — UNREACHABLE | Write to history row is **inside same transaction** as DDL. Никогда split. |
| `bsql migrate status` reflects live DB state | 3 — VERIFIED | Integration tests + property: «apply N migrations forward, N back, schema ≡ initial». |

**Cannot accidentally run migrations in production code** — `bsql migrate` — отдельный CLI binary (`bsql-migrate` crate). Runtime library не знает про DDL; `bsql::query!("CREATE TABLE …")` — compile error (macro rejects DDL kinds unless explicitly in migration crate).

---

**Additional benefits over external tools:**

- **Dual-version rollout** (rolling deploys): `Pool<V042>` и `Pool<V043>` живут параллельно; compile verifies обе ветки работают на своих schemas.
- **Rollback verified at compile time** — every `up` has typed `down`; невозможно ship one-way migration by accident.
- **Zero runtime dependency on python/node/etc.** — pure Rust CLI, single binary distribution.
- **Dry-run typed** — `bsql migrate up --dry-run` prints generated SQL + new schema type signature. Если не compile'ится → reject перед apply.

---

**Complexity honestly:** serious research — generic const strings (§`adt_const_params`), type-level tuples, macro auto-gen schema types из SQL files, CLI driver. **Несовместимо с v1.0 ship timeline.**

**Phase:** v2.0 (после v1.0 ship). Research starts Phase 6. Crate: `bsql-migrate` (separate binary + lib).

**Announcement line:** «bsql — первый Rust SQL library где migrations typechecked end-to-end: DSL → DDL → applied-schema → post-migration queries. Всё одним инструментом, без handoff.»

### §80.20. Proof-carrying tokens (C7) — research track

**Idea:** macro emits **proof tokens** — zero-sized types доказывающие свойство query'а.

```rust
// Query operates only on users table
let token: ReadToken<"users"> = bsql::query!("SELECT id, name FROM users")
    .fetch_all(&pool).await?;

// Function requires a users-read token
fn validate_user(_: ReadToken<"users">, u: &User) { ... }
```

Compiler enforces **which tables query touched** at type level. Useful for:
- Multi-tenancy enforcement ("this function only reads tenant's tables").
- Capability-based security at data access layer.
- Audit: каждый read / write touching sensitive tables carries token.

**Research-grade.** Requires substantial macro + trait machinery. Novel — no Rust SQL library has это.

**Phase:** Phase 6 POC. If successful — v1.x feature.

### §80.21. Trace context propagation (C8) — opt-in, v1.x+

**Problem.** Medium-sized production — distributed tracing (OpenTelemetry / Honeycomb / Grafana Tempo) shows service call graph. Slow queries visible in PG logs, но **не связаны** с inbound request. DBA видит slow query text, но не знает «какой endpoint вызвал?», «в каком trace context?».

**Solution — opt-in feature `tracing`:**

1. bsql автоматически reads `tracing::Span::current()` (если active).
2. Extracts `trace_id`, `span_id` из span metadata.
3. Prepends SQL comment:
   ```sql
   SELECT id, name FROM users WHERE active /* trace=abc123,span=s4 */
   ```
4. PG logs этот comment; observability tools (pgBadger, pganalyze, Grafana Cloud DB monitoring) extract `trace=` prefix → link query to distributed trace.
5. Result: медленный query viewable в полном контексте request flow.

**Это НЕ про генерацию unique IDs ad-hoc.** Это использует **существующую** distributed-tracing infrastructure, которую production system уже имеет (или не имеет и feature off).

**Feature-gated:** `feature = "tracing"` default off. Overhead ~40-80 bytes comment per query.

**Effort:** 3-5 days (standard tracing integration). **Phase 6+ / v1.x+** — «very much later» as user requested.

---

# Часть IX — Performance philosophy

## §81. Zero-cost — co-priority

Safety и performance ортогональны (§4.1). Каждая оптимизация сохраняет **все** safety guarantees. No backchannel через `unsafe` blocks, no «это только для benchmark'ов» exceptions.

Metric для regression: **2% degradation на any benchmark = block merge** до explicit user sign-off.

## §82. Binary wire protocol

PostgreSQL binary format:
- `i32::from_be_bytes([u8; 4])` — одна инструкция. Не ASCII parse.
- Timestamps — 8-byte memcpy + arithmetic.
- UTF-8 validation — SIMD (`simdutf8`).

Vs text format: 3-5× faster decode, 2× smaller wire (no conversion ASCII → numeric).

## §83. Zero-copy fetch

Row fields borrowed from response buffer (`&'buf [u8]` / `&'buf str`). Query returns `Rows<'buf>`; user iterates, extracts only what needed. Many queries (counts, EXISTS, pagination markers) never extract — pure 0-alloc.

## §84. Thread-local buffer recycling

Response buffers, column-offset vectors, decoding arenas — pooled per-thread. Second query on same thread: 0 malloc. Benchmarked ~20% gain on mixed workloads.

## §85. Pipelining

Pipelined batch INSERT: N Bind+Execute messages в one write(), then read full response burst. 2.5× faster than per-query approach. `tx.defer()` uses this automatically.

## §86. Statement cache

Per-connection LRU of prepared statements. On evict: `Close` sent to PG (server-side bookkeeping).

Cache impl: `heapless::Vec<StmtEntry, N>` with `u64` hash keys. For N < 30 (typical), Vec-scan outperforms HashMap — cache-line friendly, predictable branches.

## §87. SIMD где оправдано (feature `simd`)

`simdutf8` для UTF-8 validation — 4-8× faster than `std::str::from_utf8`. Always-on (no feature gate) — dep small, gain large.

Other candidates (feature-gated, measured before landing):
- SIMD decode for `int2` / `int4` / `int8` arrays — when benchmarked justifies.
- SIMD bytea hex encode — candidate.

## §88. PGO + LTO

Release profile:
- `lto = "fat"` — максимум cross-crate inlining.
- `codegen-units = 1` — single unit для LLVM, best optimization.
- `strip = "symbols"` — lean binary.

PGO guidance в README: user runs their app under profiling flag, then rebuilds with profile. 5-15% gain typical.

## §89. Allocator-agnostic

bsql не требует и не рекомендует specific allocator. System allocator works (1.6 MB peak RSS on 10K-query workload, 3.8× less than libpq).

User может опционально `#[global_allocator]` к mimalloc / jemallocator / snmalloc в **их** `Cargo.toml`. bsql compatible с любым.

**Нет** `features = ["alloc-mimalloc"]` etc. — это user's application decision (CREDO §4.4 — dep isn't ours to add).

## §89.1. Deep alloc minimization — per-layer policy

§4.1a даёт общее правило. Ниже — конкретизация по layer.

### Protocol crate (`bsql-pg-proto`)

- `#![no_std]` без `alloc` dep. Физически невозможно `Box`/`Vec`/`String`/`Arc` — они попросту не в scope.
- Все buffers — `heapless::Vec<u8, N>`. Все identifiers — `heapless::String<N>`.
- `PgCommand::Startup { user: Name, database: Name, ... }` — `Name = heapless::String<63>` — PG NAMEDATALEN-1, stack-allocated.
- Frame payload access — через `&[u8]` в owned `ParsedPayload` enum (tiny, Copy where possible).

### Wire codec (в `bsql-driver-postgres`)

- Response buffer — `heapless::Vec<u8, READ_BUF_CAP>` или arena (если large rows).
- Column offsets — `heapless::Vec<u32, 32>` с spill-to-heap для ширины > 32 колонок (редко).
- Decoded row fields — **references** в response buffer (`&'buf str`, `&'buf [u8]`). User decides когда `.to_owned()`.
- Bind параметры — **arena-alloc** через `bsql-arena` thread-local. `BindTemplate` с `encode_at` для пере-использования.

### Pool / Client

- Pending replies map — `heapless::FnvIndexMap<ReplyId, Sender, MAX_INFLIGHT>` внутри task state. Не HashMap.
- SQL strings в commands — `Arc<str>` (shared reference, refcount increment on clone — не alloc).
- Connection config — `Arc<PgConfig>` (parsed once at pool creation, shared across slots).

### Macros

- Generated struct field types — `&'row str` (borrowed from decoder output) где возможно. User pays `.to_owned()` explicitly на границе.
- `RawRow` — wrapper над `&'buf [u8]` с lazy column split. `.get(i)` — `&'buf [u8]`. Alloc только на `.to_vec()`.
- Generated stmt names — `&'static str` (compile-time `const`) или `[u8; 16]` stack array.

### What stays alloc (acceptable)

- `Vec<Row>` возврат из `fetch_all` — user-owned terminal API. User explicit chose to collect.
- `String` fields в user-facing structs when column is `TEXT` / `VARCHAR` — user semantics.
- Error message strings — cold path.
- Offline cache (de)serialize — infrequent, happens at `cargo build` or Pool::connect.
- Configuration parsing — happens once.

## §89.2. Lazy evaluation

«Не делай, пока не попросили».

### Row field decode

```rust
// Bad (eager):
struct Row { id: i32, name: String, data: Vec<u8> }
// All three decoded into owned types on every row.

// Good (lazy):
struct Row<'buf> { payload: &'buf [u8], cols: &'buf [ColMeta] }
impl<'buf> Row<'buf> {
    pub fn id(&self) -> Result<i32, DecodeError>;      // decodes only when called
    pub fn name(&self) -> Result<&'buf str, DecodeError>;
    pub fn data(&self) -> Result<&'buf [u8], DecodeError>;
}
```

Query `SELECT id, name, huge_blob FROM t` — user reads только `id`, `name` в коде. `huge_blob` никогда не декодируется. Cost = 0 для unused field.

Implementation: macro-generated struct держит `payload: &'buf [u8]` + generated getter per column, getters call appropriate decoder on demand.

### Error payload lazy formatting

```rust
pub struct ErrorResponse<'buf> { payload: &'buf [u8] }
impl<'buf> ErrorResponse<'buf> {
    pub fn severity(&self) -> &'buf str { ... }  // parse lazy
    pub fn code(&self) -> &'buf str { ... }
    pub fn message(&self) -> &'buf str { ... }
    // ...
}

// BsqlError variant stores enum-typed parsed fields only when user matches
pub enum QueryError {
    ServerError { severity: &'static str, code: SqlState, message: Box<str> },
    // message allocated only when specific variant triggered, user pattern-matches
}
```

### Protocol session params

ParameterStatus frames (timezone, server_version, etc.) consumed by wrapper, stored in `Option<Arc<SessionParams>>`. Built только on first `.session_params()` call.

### Stream consumption

Row stream yields one row at a time. User processes & drops before next. Never materialize all rows together unless `.fetch_all()` explicitly called.

## §89.3. Batching beyond §85 pipelining

§85 описывает per-transaction pipelining. Дополнительные batching opportunities:

### Batched channel recv

```rust
// run_io loop:
loop {
    tokio::select! {
        biased;
        _ = cmd_rx.recv() => {
            // Drain more без awaiting — batch up to N commands in one burst.
            let mut batch = heapless::Vec::<Command, 8>::new();
            while let Ok(cmd) = cmd_rx.try_recv() {
                if batch.push(cmd).is_err() { break; }
            }
            dispatch_batch(batch).await;
        }
        // ... socket readable arm ...
    }
}
```

Backpressure: channel cap bounds batch size. Latency vs throughput tunable per-workload.

### Vectored socket writes

```rust
use std::io::IoSlice;
let bufs = [IoSlice::new(parse_msg), IoSlice::new(bind_msg), IoSlice::new(execute_msg), IoSlice::new(sync_msg)];
stream.write_vectored(&bufs).await?;
```

Single syscall. Already used in v0.27; preserved.

### Chunked socket reads

```rust
let n = stream.read(&mut chunk).await?;  // up to 64KB
proto.feed_bytes(&chunk[..n]);           // parses multiple frames in one go
```

One `.read()` → often N frames processed. Latency ≈ single-frame case; throughput scales.

### Statement cache warmup batching

```rust
pool.builder().warmup(&["SELECT 1", "SELECT id FROM users WHERE id = $1", ...])
```

All warmup statements prepared in **one** PREPARE batch at connection open. Not per-first-execute.

## §89.4. Inline / cold hints

### Hot path

- `#[inline]` — across crate boundaries where LTO may not reach (pub functions in non-final binary).
- `#[inline(always)]` — strictly for 1-3 LoC trivially-inlineable getters / bounds-check bypasses.
- Default (no attribute) — let compiler decide; usually right.

### Cold path

- `#[cold]` on error-return functions — compiler reorders basic blocks, I-cache stays tight on happy path.
- `#[inline(never)]` on large error-formatting / error-emitting functions — keep them as standalone procedures.

### Measurement

Every `#[inline]` / `#[cold]` annotation verified with `cargo asm` diff. If generated code unchanged → remove attribute (noise). Kept annotations correlate with benchmark win > 1%.

## §89.5. Lookup tables (LUT)

### Bytea hex encode / decode

```rust
static HEX_ENCODE: [[u8; 2]; 256] = {
    let mut table = [[0u8; 2]; 256];
    let mut i = 0;
    while i < 256 {
        table[i] = [to_hex_char((i >> 4) as u8), to_hex_char((i & 0xF) as u8)];
        i += 1;
    }
    table
};
```

Single indexed load instead of 2× arithmetic + branch.

### Dispatch tables

Tag → handler function pointer:

```rust
type FrameHandler = fn(&mut PgProtocol, &[u8]) -> DispatchOutcome;
static HANDLERS: [Option<FrameHandler>; 128] = {
    let mut t = [None; 128];
    t[b'R' as usize] = Some(handle_auth);
    t[b'K' as usize] = Some(handle_backend_key);
    // ...
    t
};
```

Branch chain → indexed jump. ~2-5 cycles faster per frame.

### OID → decoder

```rust
static DECODER_FOR_OID: [Option<DecoderFn>; PG_OID_RANGE_COMMON] = { ... };
```

Common OID range (0-10000 covers most types) → direct array index. Uncommon OIDs fallback to match.

### Base64 / ASCII tables

SCRAM parsing, cleartext pwd, future auth mechanisms.

Implementation: `const [T; N]` — build-time computed, 0 runtime cost.

## §89.6. Cache-line aware layout

### PgProtocol struct

```rust
#[repr(C)]
pub struct PgProtocol {
    // Cache line 1 (hot — accessed every feed/push):
    state: ProtoState,              // ~32 bytes
    consumed: usize,                 // 8 bytes
    read_buf_len: usize,             // 8 bytes (redundant with read_buf.len() but fast)
    
    // Cache line 2+ (cold — accessed rarely):
    read_buf: ReadBuf,
    stmt_cache: StmtCache,
    session_params: Option<Arc<SessionParams>>,
    schema_fingerprint: Option<[u8; 32]>,
    _not_sync: PhantomData<Cell<()>>,
}
```

### False-sharing prevention

- Atomic counters (metric tracking, pool stats) — `#[repr(align(64))]` wrapper to isolate на own cache line. Prevents different cores contending через L1 ping-pong.

### Verification

`perf stat -e L1-dcache-load-misses,L1-dcache-loads` in bench suite. Regression indicator. Layout changes reviewed with asm diff.

## §89.7. Branch prediction

### Happy-path straight

```rust
// Bad:
if error_condition { emit_error(); return; }
// happy path
```

Compiler may predict error_condition 50/50.

```rust
// Good:
match check() {
    Ok(()) => {
        // happy path — majority weight
    }
    Err(e) => {
        #[cold]
        fn emit(e: Error) { ... }
        emit(e);
        return;
    }
}
```

`#[cold]` on inner function → compiler moves emit basic block to end of function. Happy path contiguous in I-cache.

### `std::hint::cold_path()` (stable 1.83+)

```rust
if very_rare_condition {
    std::hint::cold_path();
    // compiler treats branch as unlikely
    ...
}
```

Use inside hot functions where annotating separate function costly.

### No `likely!` / `unlikely!` crates

External deps для marginal clarity. Compiler's own hints + `Err` / `None` / `#[cold]` uniformly cover what we need.

## §89.8. Const evaluation where possible

### Wire constants

```rust
const TAG_SYNC: u8 = b'S';
const SYNC_WIRE_BYTES: [u8; 5] = [TAG_SYNC, 0, 0, 0, 4];
const MAX_FRAME_LEN_FIELD: usize = READ_BUF_CAP.saturating_sub(1);
const _: () = assert!(MAX_FRAME_LEN_FIELD >= 5);
```

Build-time validated. Runtime — literal in text section.

### Generated code from macro

Macro emits `const` для everything that doesn't depend on runtime:
- Stmt hash → `const STMT_HASH: u64 = 0xabc123def456;`
- Stmt name → `const STMT_NAME: &[u8; 18] = b"s_abc123def456_000";`
- Param OIDs array → `const PARAM_OIDS: [u32; 3] = [23, 25, 16];`
- Column count → `const N_COLS: usize = 4;`

Generated binary carries these as literals. Zero runtime initialization cost.

### `const fn` where feasible

- Buffer size calculations.
- Hash fingerprint comparison (byte-wise).
- OID lookup (если PG OID range sufficiently dense для const table).

### What's NOT const (yet)

- SHA-256 / HMAC — crypto crates don't expose `const fn` computation. Computed at macro-time instead and emitted as literal `[u8; 32]`.
- `TryFrom` in `const` context — not stable in current Rust (§117 Q-10 если нужно). Use `try_from` runtime (inevitable single call).

## §89.9. Dep policy — no JSON сейчас, `sonic-rs` если понадобится

bsql **не имеет** JSON dependency. Пути где JSON мог бы появиться — все альтернативы выбраны:

| Потенциальный use | Наш выбор | Почему не JSON |
|---|---|---|
| Offline cache | `bitcode` | 50× быстрее, 3× compactнее |
| Protocol wire | PG binary | Native, 3-5× faster decode |
| Config | URL parser + builder | No structured config нужен |
| Error payloads | Typed enum | Type-safe pattern-match |
| Logging | `log` crate с user-chosen subscriber | Agnostic |
| Telemetry / traces | opt-in `tracing` (Phase 5) | Structured spans, не JSON wire |

**Если в будущем JSON понадобится** (например, PG JSONB column в text representation с хотимой structured access, или OpenTelemetry JSON exporter):

**Use `sonic-rs` (preferred) or `simd-json`. Never `serde_json`.**

Rationale:
- `sonic-rs` — RapidJSON C++ port в pure Rust. SIMD-ускоренный. 2-10× faster than `serde_json` на parse / stringify.
- `simd-json` — SIMD-powered DOM/event parser. Similar perf.
- `serde_json` — byte-at-a-time, heavy allocation per parsed value. Incompatible с §89 policy.

**Lazy JSON boundary:** если пользователь запросил `JSONB` column, bsql возвращает `&'buf [u8]` (zero-copy reference into response). User передаёт в `sonic_rs::from_slice` сам (bsql не навязывает JSON parser choice). Мы только обещаем **borrow stable until next feed_bytes**.

### Rejected JSON-adjacent deps

- `serde_json` — slow, heavy allocation.
- `json` crate — similar limitations.
- Custom JSON parser — not worth maintaining when `sonic-rs` / `simd-json` cover it at SIMD speeds.

## §89.10. `std::simd` portable decoder (Phase 5 research)

`std::simd` (portable SIMD, stable trajectory через `core::simd` — current MSRV 1.94 has partial, full stable expected v1.95+) позволяет хоткий PostgreSQL binary-format decode **без unsafe** и **без target_feature pragmas**.

**Целевые hot paths:**

| Op | Scalar path | SIMD gain (estimated) |
|---|---|---|
| `i32` array decode (PG `int4` OID 23, big-endian → native) | Per-element `u32::from_be_bytes` | 4-8× (AVX2: 8× u32 в 256-bit reg) |
| `i64` array decode (`int8`) | Per-element byte swap | 2-4× |
| `TEXT`/`VARCHAR` UTF-8 validation (bulk rows) | Byte-at-a-time walk | 4-16× (`simdutf` algo) |
| Row-length prefix batch extract (RowDescription walk) | Serial read | 2-4× |

**Policy:**
- Gate behind `feature = "simd"` (default-on starting v1.1 once stable).
- Fallback scalar path тестируется equivalently в CI (differential: `simd_decode(bytes) == scalar_decode(bytes)` property test, 10⁶ cases).
- Zero-unsafe: `core::simd` API всё safe-abstracted.
- Benchmark gate: merge blocked если SIMD path **не ≥2×** over scalar на target architecture (AVX2 / NEON).

**Why not `packed_simd` / `simdeez` deps:** external SIMD crates — either deprecated или bring их own unsafe. `std::simd` — part of stdlib, maintained by stdlib team.

## §89.11. Memory prefetching в row decode loops (Phase 5 research)

PostgreSQL `DataRow` messages читаются последовательно; row N+1 уже в kernel TCP buffer к моменту когда парсим row N. Используя `std::hint::spin_loop` (+ eventually `core::intrinsics::prefetch` когда stable), можно префетч'ить следующую cache line пока декодим текущую.

**Pattern (psuedo):**

```rust
for row in datarow_iterator {
    // Hint CPU: подтяни next row bytes в L1 cache
    hint::prefetch_read(next_row_start_ptr, hint::Locality::L1);
    decode_row(row)?;  // Эти bytes уже в L1 к моменту decode
}
```

**Measured wins (industry baselines, e.g. ClickHouse, DuckDB):** 10-25% throughput на large SELECT (10⁴+ rows).

**Gate:** `feature = "prefetch"`. Off by default until stable prefetch intrinsic. Differential test ensures same bytes produced.

## §89.12. Branchless NULL-indicator decode (Phase 5 research)

PostgreSQL DataRow encodes NULL как length `-1` (i32). Naive decode:

```rust
let len = read_i32_be(bytes);
let value = if len == -1 { None } else { Some(decode(&bytes[..len])) };
```

Branch predictor struggles when NULLs random-interleaved (50/50 distributions часто в realistic data).

**Branchless variant:**

```rust
let len = read_i32_be(bytes);
let is_null_mask = (len >> 31) as u32;  // -1 → 0xFFFF_FFFF, ≥0 → 0x0
let len_clamped = (len as u32) & !is_null_mask;  // 0 if NULL, else len
// Read len_clamped bytes unconditionally; wrap в Option based on mask
```

**Measured win** (internal / published industry benchmarks): 2-5% row decode throughput on NULL-heavy data.

**Policy:** применяется только где bench показал >3% regression если убрать. Иначе — branchy version (readable). Perf-critical type decoders (i32, i64, text) get branchless. `Row::try_get<T>()` user-facing API остаётся readable — branchless живёт в internal decoder fns.

## §89.13. Columnar row decode layout (Phase 5+ research)

**Idea:** для bulk `.fetch_all()` — decode as **Struct-of-Arrays (SoA)** не **Array-of-Structs (AoS)**:

```rust
// AoS (current plan) — Vec<Row>
struct Rows { rows: Vec<Row> }           // Row = { Vec<Value> ... }

// SoA variant
struct ColumnarRows {
    id_col: Vec<i32>,        // all i32 contiguous
    name_col: Vec<String>,   // all strings contiguous
    email_col: Vec<Option<String>>,
}
```

**Benefits:**
- SIMD-friendly для post-query analytics (sum of `id_col`, etc.).
- Better cache behavior для column-wise consumers.
- Closer match к PG's binary encoding (columns already grouped в certain scenarios).

**Tradeoffs:**
- Не подходит для row-at-a-time streaming API (`.fetch_iter()`).
- Type erasure challenge (columns имеют разные types).

**API shape (tentative):**

```rust
let columns: Columns<(i32, String, Option<String>)> = query!(...)
    .fetch_columnar(&pool).await?;
let sum: i64 = columns.col_0().iter().sum();  // SIMD-vectorizable
```

**Phase:** v1.x+ after v1.0 ship. Research: does macro type-derive generate competent `Columns<T>` без explosion.

## §89.14. Niche-optimized `Result` layout (Phase 5 research)

Rust `Result<T, E>` size = `max(size_of::<T>(), size_of::<E>()) + discriminant`. Для hot paths (`fn read_i32(&self) -> Result<i32, PgError>`) это может удвоить return size.

**Optimization:**
- `PgError` — enum с variants carrying references / boxed data. `size_of::<PgError>() ≤ 16 bytes` обеспечивается invariant'ом (`const_assert!(size_of::<PgError>() <= 16)`).
- Для single-field types — использовать niche (`NonZeroI32`, `NonZeroU32`) где semantic позволяет.
- `Result<(), Fatal>` — guaranteed 1-word через `Fatal: !Unpin + #[repr(transparent)]` around `BsqlError`.

**Measurement:**
- `cargo asm` inspection показывает `RAX/RDX` pair returns vs stack spill.
- Bench: decode_loop throughput delta, target >2% wins где applicable.

**Policy:** applied только где measured. Не premature optimization — run cargo asm first, identify spill, then niche.

---

# Часть X — Verification infrastructure

## §90. The verification stack

Каждая layer закрывает specific class:

| Layer | Closes | Run frequency |
|---|---|---|
| forbid bundle (compile) | Tier 1 invariants | Every `cargo build` |
| exhaustive match | Tier 1 state machine | Every `cargo build` |
| Bounded type bounds | Tier 1 / Tier 2 overflow | Every `cargo build` |
| Const asserts | Tier 1 constants | Every `cargo build` |
| proptest (§91) | Parser invariants under random input | Every `cargo test` (1024 cases) + nightly (10⁵+) |
| Loom (§92) | Concurrent interleavings | Nightly (hours) |
| cargo-fuzz (§93) | Parser corpus-guided fuzz | Nightly continuous |
| cargo-mutants (§94) | Test suite quality | Nightly (kill-rate ≥ 85%) |
| cargo-deny (§95) | Supply-chain (CVE, license, duplicates) | Every PR |
| cargo-vet (§95) | Human-reviewed dep trust | Every PR (incremental) |
| Differential (§96) | Spec-conformance vs reference impl | Nightly |
| Reproducible builds (§97) | Supply-chain substitution | Release builds |

## §91. proptest

Generators для bounded input (random bytes, random op sequences). Properties:
- Parser never panics.
- State always in defined variant.
- Output bounded.
- Errored is sticky.
- Invariants hold across arbitrary op sequences.

```rust
proptest! {
    #![proptest_config(ProptestConfig { cases: if cfg!(CI_NIGHTLY) { 100_000 } else { 1024 }, .. })]
    #[test]
    fn feed_bytes_never_panics(bytes in prop::collection::vec(any::<u8>(), 0..512)) { ... }
    // ...
}
```

## §92. Loom

Loom exhaustively checks concurrent harness. Specifically для `run_io` wrapper:

```rust
#[cfg(loom)]
#[test]
fn run_io_no_deadlock_no_data_race() {
    loom::model(|| {
        let (client, handle) = Client::<TestBackend>::spawn(...);
        let sender = tokio::spawn(async move { client.ping().await });
        let canceler = tokio::spawn(async move { drop(sender); });  // racing drop
        // loom will try every interleaving
    });
}
```

Bounded by `LOOM_MAX_PREEMPTIONS`; nightly runs at higher bounds.

## §93. cargo-fuzz

libFuzzer-backed. Targets:
- `fuzz_targets/pg_proto_feed_bytes.rs` — `PgProtocol::feed_bytes` on random bytes.
- `fuzz_targets/macro_validator.rs` — `query!` macro on random SQL strings.
- `fuzz_targets/bitcode_roundtrip.rs` — cache serialize/deserialize.

Corpus committed under `fuzz/corpus/`. Nightly CI runs ~4 hours per target.

## §94. cargo-mutants

Mutates operators (`+` → `-`, `<` → `>`, etc.) and drops statements. Runs test suite per mutation. Kill rate = % of mutations caught.

Policy: **kill rate ≥ 85%** для wire-path crates. PR breaking threshold — block.

## §95. cargo-deny / cargo-audit / cargo-vet

- **cargo-deny** — CI gate. Policy: RustSec advisory clean, whitelist licenses, no duplicate versions. Config in `deny.toml`.
- **cargo-audit** — subset (CVE-only); redundant с `cargo-deny`, не используется separately.
- **cargo-vet** — human audit trail для каждой dep version. `audits.toml` в repo. Shared trust с Mozilla / Google / другими — import их audit.

## §96. Differential testing (nightly CI)

```rust
#[bsql::diff_test]
async fn select_one_row_equivalent(pool: bsql::Pool) {
    let sql = "SELECT id, login FROM users WHERE id = $1";
    let bsql_row = bsql::query!(sql, 1i32).fetch_one(&pool).await?;
    let tpg_row = tokio_postgres::query_one(sql, &[&1i32]).await?;
    assert_eq!(bsql_row.id, tpg_row.get::<_, i32>(0));
    assert_eq!(bsql_row.login, tpg_row.get::<_, String>(1));
}
```

Macro `#[bsql::diff_test]` spawn'ит both bsql pool AND tokio-postgres client, compares results. Catches subtle spec-misunderstanding bugs.

## §97. Reproducible builds

`cargo build` → byte-identical output across machines (given same MSRV, same deps, same environment). Catches supply-chain substitution (someone swapped dep on crates.io between our build and user's).

Tool: `cargo-repro` or manual `diffoscope`.

## §98. CI matrix

GitHub Actions (`.github/workflows/`):

### `pr.yml` — on every PR (fast, minutes)
- `cargo build --workspace --all-features`.
- `cargo test --workspace --lib --all-features` — unit tests only, no DB.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings`.
- `cargo fmt --all --check`.
- `cargo deny check`.
- proptest 1024 cases.

### `integration.yml` — on PR, matrix
- PG versions: 15, 16, 17, 18.
- Rust versions: MSRV (1.94), stable, nightly.
- OS: ubuntu-latest, macos-latest.
- `cargo test --workspace --all-features` with `BSQL_DATABASE_URL` set.

### `nightly.yml` — nightly
- proptest 100_000 cases.
- cargo-fuzz 4 hours per target.
- cargo-mutants full run, assert kill rate ≥ 85%.
- Loom harness.
- Differential tests vs tokio-postgres + libpq.
- Reproducible build check.

### `release.yml` — on tag push
- Full matrix above.
- Reproducible build verification.
- Changelog gen.
- Tagged publication to crates.io.
- Docs.rs build verification.

---

# Часть XI — Dependencies

## §99. Dependency policy

Каждая dep проходит gate перед добавлением:

1. **Can we write in < 200 LoC?** If yes, write.
2. **Is it maintained?** Last commit < 12 months, issue response < 2 weeks.
3. **`default-features = false`.** Only features actually used.
4. **Pinned version.** No `"*"`, no `"^"` wide range. Pinned `x.y.z` or tightest possible.
5. **Audit-reviewed.** `cargo-vet` audit note required.
6. **Transitive count.** Adding a dep that pulls 20 transitive deps — costly.

Periodic audit (every release): can any dep be removed or replaced в-house?

## §100. Runtime deps — complete list with justification

| Dep | Version | Features | Crates using | Justification |
|---|---|---|---|---|
| `tokio` | `1.x` | `sync`, `rt` в backend; `net`, `time`, `macros`, `fs` в driver-postgres | bsql-backend, bsql-core, bsql-driver-postgres, bsql-driver-sqlite | Async runtime. Cannot plausibly write. |
| `heapless` | `0.8` | `default-features = false` | bsql-pg-proto | Bounded Vec/String/Map for no_std. ~5 KLoC well-maintained. Alternative: write ourselves — rejected, hot path needs battle-tested. |
| `rustls` | `0.23` | `ring`, `std` | bsql-driver-postgres | TLS. libsslmao нет в Rust; OpenSSL binding adds C dep. rustls pure-Rust. Hard-pin `ring` crypto provider (see §102). |
| `webpki-roots` | `1.0` | `default-features = false` | bsql-driver-postgres | CA bundle для rustls. |
| `tokio-rustls` | `0.26` | `ring` | bsql-driver-postgres | Tokio adapter для rustls. |
| `rustls-pemfile` | `2` | `std` | bsql-driver-postgres | PEM parsing for custom certs. |
| `ring` | indirect | — | bsql-driver-postgres | rustls crypto provider. |
| `rapidhash` | `4.4` | — | bsql-pg-proto (Phase 2+), bsql-driver-postgres, bsql-macros | Fastest hash for short-medium strings. 200 LoC no deps. Stmt names, cache keys. |
| `bitcode` | `0.6` | `derive` | bsql-macros, offline cache | Binary serialization. 50× faster than JSON. |
| `sha2` | `0.10` | `default-features = false` | bsql-macros (schema fingerprint), bsql-pg-proto (SCRAM) | SHA-256. |
| `zeroize` | `1.x` | `zeroize_derive` | bsql-core (Sensitive), bsql-pg-proto (password buffers) | Zero-on-drop. |
| `subtle` | `2.x` | `default-features = false` | bsql-pg-proto (SCRAM signature) | Constant-time equality. |
| `simdutf8` | `0.1` | — | bsql-driver-postgres | SIMD UTF-8 validation. 4-8× over std. |
| `libsqlite3-sys` | `0.30` | `bundled` opt | bsql-driver-sqlite | SQLite FFI. |
| `crossbeam-channel` | `0.5` | `default-features = false` | bsql-driver-sqlite | Sync channels inside SQLite pool (not Tokio). |
| `syn` / `quote` / `proc-macro2` | latest | relevant | bsql-macros | proc-macro foundations. |
| `fs2` | `0.4` | — | bsql-macros (cache lock) | Cross-platform file locking. |
| `log` | `0.4` | `default-features = false` | bsql-core, bsql-driver-postgres | Logging. Optional — можно через `tracing`, open §117 Q-5. |

### Feature-gated (optional)

| Dep | Feature | Purpose |
|---|---|---|
| `time` | `time` | TIMESTAMPTZ, DATE, TIME |
| `chrono` | `chrono` | Alternative to `time` |
| `uuid` | `uuid` | UUID |
| `rust_decimal` | `decimal` | NUMERIC |

## §101. Dev-deps

| Dep | Purpose |
|---|---|
| `proptest` | Property-based testing. |
| `loom` | Concurrency model checker. |
| `criterion` | Benchmarking. |
| `tokio-test` | Async test utilities. |
| `trybuild` | Compile-fail tests for macros. |
| `tempfile` | Test cache setup. |
| `rcgen` | Test TLS cert generation. |

## §102. Rejected deps

| Rejected | Why rejected |
|---|---|
| `serde` / `serde_json` | Heavy. We use `bitcode`; schema fingerprint uses `sha2` directly. |
| `anyhow` / `thiserror` | Manual error types — typed pattern-matchable. `thiserror` might be accepted at some point for derive ergonomics — §117 Q-6. |
| `futures` ecosystem (except tokio pieces) | Tokio native APIs sufficient. `futures-util` combinators add surface without need. |
| `async-trait` | Stabilized async fn in traits (1.75+) removes need. |
| `parking_lot` | Std Mutex sufficient; parking_lot adds dep without measurable win на hot paths после contention optimizations в recent std. |
| `smallvec` | `heapless` covers; keeps one bounded-vec library. |
| `bytes` | We use `heapless::Vec` + slices. `bytes::BytesMut` adds indirection. |
| `aws-lc-rs` | Rejected in favour of `ring`. Hard-pin `ring` via `default-features = ["ring"]` on rustls — avoids process-level `CryptoProvider` panic when cargo feature unification pulls both. |
| `libpq` FFI | We implement PG wire in pure Rust (`bsql-pg-proto`). libpq C dep adds build complexity + attack surface. |

## §103. Supply-chain audit

- `cargo-deny` config: deny CVEs, whitelist licenses (MIT, Apache-2.0, BSD, ISC, MPL-2.0), no duplicates, no banned authors.
- `cargo-vet` audit trail: every dep version has reviewer + notes in `audits.toml`.
- Import Mozilla / Google / Embark audits where they overlap our dep tree.
- `.cargo/audit.toml` hook for RustSec feed.

---

# Часть XII — Roadmap

## §104. Phases, not tasks

Roadmap описывает **фазы** — узкие milestone'ы с clear acceptance criteria. Не step-by-step task list. Внутри фазы — инженерные решения по ходу, с §3-ориентиром.

Фаза закрыта = acceptance criteria met + CREDO §0 matrix walked + user sign-off.

## §105. Phase 0 — Foundation (done)

✅ reset (this commit).
✅ reforge.md (this file).

Phase 0 не имеет Rust-кода. Exit criteria — reforge.md agreed upon, empty workspace committed.

## §106. Phase 1 — PG wire layer

Acceptance:
- `bsql-pg-proto` crate has full PG protocol state machine: TCP connect init, SCRAM-SHA-256 auth (+ channel binding / PLUS), startup handshake, Query, Execute (INSERT/UPDATE/DELETE), SimpleQuery, Begin/Commit/Rollback, Listen/Unlisten, QueryStream, COPY FROM STDIN, CancelCurrent, Terminate. All state-as-data.
- `bsql-backend` crate: Backend trait, Client<B>, channel architecture.
- `bsql-driver-postgres` crate: PgBackend impl, async run_io, TLS, binary codec.
- Live PG ping round-trip integration test passes.
- Live PG SELECT round-trip integration test passes.
- All of §52 + §53 safety matrix rows hold.
- proptest на feed_bytes passes 100K cases.
- Loom harness on run_io passes.

**Sub-phases:**
- 1a — bsql-pg-proto skeleton + Ping flow.
- 1b — SCRAM-SHA-256 в pure-sync (hardest — ~300 LoC HMAC-SHA256 + PBKDF2 + base64 + nonce generation).
- 1c — Query / Execute / post-auth chain / basic codec.
- 1d — Streaming, COPY, LISTEN/NOTIFY.
- 1e — bsql-backend + PgBackend + run_io + live ping integration.
- 1f — full query integration test (real SELECT against live PG, using raw codec; NO macro yet).

## §107. Phase 2 — Macros + compile-time validation

Acceptance:
- `bsql-macros` crate: `query!`, `pg_enum`, `sort`, `test`, `connect` macros.
- Online mode: connect to live PG при `cargo build`, PREPARE + DESCRIBE, extract types.
- Offline mode: bitcode cache read/write, manifest locking, schema fingerprint.
- 50+ nullability inference patterns with tests.
- Compile-fail tests (trybuild) pin error message quality for all documented failures.
- `bsql::query!("SELECT id FROM users WHERE id = $id: i32").fetch_all(&pool)` works end-to-end.

**Sub-phases:**
- 2a — Minimal compile-time PG client inside macro (TCP + SCRAM + PARSE + DESCRIBE).
- 2b — SQL parser, parameter extraction, OID → Rust type mapping.
- 2c — Nullability inference (20 patterns).
- 2d — Remaining nullability patterns (50 total).
- 2e — Optional clauses / sort enums / UNNEST / COPY paths.
- 2f — Offline cache + manifest + fingerprint.
- 2g — Attribute macros (pg_enum, sort, test, connect).
- 2h — Error diagnostic quality + compile-fail tests.

## §108. Phase 3 — Pool + Transaction + listener

Acceptance:
- `bsql-core` crate: generic `Pool<B>`, `Transaction<'pool, B>`, `Listener`.
- LIFO, fail-fast, health checks, stale eviction.
- read/write splitting via replica_url.
- Singleflight feature.
- Warmup.
- Schema fingerprint runtime check.
- `bsql` facade crate with type aliases.
- End-to-end: full-stack query through macro → pool → driver → live PG.

## §109. Phase 4 — SQLite backend

Acceptance:
- `bsql-driver-sqlite` crate: SqliteBackend, FFI wrapper (single unsafe module), spawn_blocking shim.
- Same Backend trait, same Client<B> mechanism.
- `bsql::SqlitePool` type alias в facade.
- All §54 safety matrix rows hold.
- SQLite-specific features (WAL, mmap, STRICT tables, foreign keys).
- SQLite param type checking в macro (§77).
- Parity test suite (where semantically applicable) against PG path.

## §110. Phase 5 — Killer features

Acceptance:
- N+1 detection (§66) — feature-gated, working.
- Compile-time EXPLAIN (§67) — feature-gated, working.
- Migration safety check (§68) — `bsql migrate --check` CLI.
- Compile-time URL validation (§72) — `Pool::connect!`.
- Statement warmup integration (§76).
- Breakthrough features from §80 — at least 2 landed (e.g., compile-time cost budget §80.1, idempotency markers §80.2).

## §111. Phase 6 — Verification infrastructure

Acceptance:
- CI matrix (§98) configured: pr.yml, integration.yml, nightly.yml, release.yml.
- cargo-deny passing with full policy.
- cargo-vet audit trail populated for all deps.
- proptest 100K-case nightly passes.
- cargo-fuzz targets defined, nightly corpus growing.
- cargo-mutants nightly, kill rate ≥ 85%.
- Loom harnesses for each concurrent-code module.
- Differential tests vs tokio-postgres.
- Reproducible builds verified on release.

## §112. Phase 7 — Polish and v1.0 ship

Acceptance:
- README rewritten with benchmarks.
- docs.rs complete.
- Benchmarks against libpq, sqlx, tokio-postgres, diesel — published.
- v1.0 tag pushed to crates.io.
- Release blog post.

## §113. Out of scope for v1.0 (v1.1+ backlog)

- MySQL backend.
- ClickHouse backend.
- `tokio-uring` integration (Linux kernel-bypass I/O).
- WASM compilation path.
- Compile-time deadlock detection (§80.4).
- Typed migrations (§80.5).
- Custom async executor (only if tokio shown inadequate).

---

# Часть XIII — Appendices

## §114. Glossary

- **Sans-I/O:** architectural pattern where protocol logic is pure sync state machine, separated from async I/O transport. See §7.1.
- **State-as-data:** encoding in-flight correlators inline in state enum variants. See §7.2.
- **Typestate:** encoding protocol states as type parameters. See §7.3.
- **Sealed trait:** trait only implementable in defining crate. See §7.5.
- **Tier 1/2/3:** three-tier safety mandate. See §3.
- **Backend:** generic DB-specific impl of Backend trait. Per-backend crate + impl.
- **Protocol:** per-backend wire-protocol state machine (e.g., PgProtocol).
- **Transport:** async-capable I/O channel (e.g., TcpStream, TlsStream, FFI handle).
- **Pool:** connection pool — generic over Backend.
- **Client<B>:** thin handle to a backend-owned task.
- **ReplyId:** opaque handle for correlating pushed command ↔ reply.
- **Action:** side-effect directive from protocol state machine to async wrapper.
- **Offline cache:** bitcode-serialized per-query validation result, committed to git.
- **Schema fingerprint:** SHA-256 of sorted PG catalog snapshot.

## §115. References

- PostgreSQL protocol: [https://www.postgresql.org/docs/current/protocol.html](https://www.postgresql.org/docs/current/protocol.html)
- SCRAM-SHA-256: [RFC 7677](https://datatracker.ietf.org/doc/html/rfc7677)
- SQLite wire: N/A (in-process)
- rustls docs: [https://docs.rs/rustls/](https://docs.rs/rustls/)
- Loom paper: [https://github.com/tokio-rs/loom](https://github.com/tokio-rs/loom)
- Proptest paper: [https://altsysrq.github.io/proptest-book/](https://altsysrq.github.io/proptest-book/)
- cargo-vet (Mozilla): [https://mozilla.github.io/cargo-vet/](https://mozilla.github.io/cargo-vet/)
- sans-I/O в Python: [https://sans-io.readthedocs.io/](https://sans-io.readthedocs.io/)
- rustls sans-I/O core: `ConnectionCommon`, see `rustls/src/conn.rs`
- h2 sans-I/O: [https://docs.rs/h2/](https://docs.rs/h2/)

## §116. Open questions (to decide during implementation)

Вопросы зафиксированы здесь, решаются по мере фаз. Каждый — either resolved в commit'е который реализует соответствующую часть, или escalated в reforge.md revision.

### Q-1. `raw_query` as `unsafe fn`?

Split `raw_query(sql: &'static str)` (literal only — no runtime composition) vs `raw_query_dynamic(sql: &str)` marked `unsafe fn`? Latter forces explicit acknowledgment of SQL-injection risk. Benefit: compiler stops suggesting `raw_query` for dynamic use cases. Cost: API surface.

Decide: Phase 2e.

### Q-2. `#[bsql::connect]` attribute macro?

Attribute for `PoolBuilder` templating:
```rust
#[bsql::connect("postgres://...", max_size = 20, ...)]
struct AppPool;  // generates Pool ctor
```

Value vs `Pool::connect!()` marginal. Decide: Phase 3.

### Q-3. SCRAM share между macro crate и driver?

Macro needs SCRAM for online-mode DB connect. Driver has SCRAM in `bsql-pg-proto`. Options:
1. Duplicate — cheap implementation-wise, 300 LoC SCRAM в both crates.
2. Extract в `bsql-scram` crate — pure helper.
3. `bsql-pg-proto` exposes SCRAM helper; macro depends on it (but macro should not depend на runtime... circular via dev-only?).

Decide: Phase 2a.

### Q-4. Log crate: `log` vs `tracing`?

`log`: mature, simple, ubiquitous. `tracing`: structured, async-aware, richer. Adopting `tracing` тащит more deps + user must opt into subscriber.

Decide: Phase 3.

### Q-5. Workspace resolver version?

Edition 2024 defaults to resolver 3. Keep? Decide: Phase 0 (already set `resolver = "3"` в Cargo.toml — accepting).

### Q-6. `thiserror` for error derive?

Benefit: derive macros reduce error-impl boilerplate. Cost: dep. Decide: когда manual impl'ы begin to repeat в 3+ crates.

### Q-7. `#[diagnostic::do_not_recommend]` stabilization

Stable 1.85+. We use 1.94. Safe to use. Decide: Phase 2h — apply to `raw_query` и `BsqlError::Other`.

### Q-8. Capability tokens для Pool::acquire — opt-in?

`Pool<B, WithCap>` vs `Pool<B>`. Adds API ceremony. For advanced users только. Decide: Phase 3.

### Q-9. Async closures в user-facing API?

Rust 1.85+ stabilizes async closures. Methods like `for_each(&pool, async |row| { ... })` — pretty. Worth using vs `impl FnMut(...) -> impl Future<...>`? Decide: Phase 1f.

### Q-10. `const PG_VERSION_RANGE: (u32, u32) = (100_000, 180_000);` — what about PG 19+?

We validate server version at connect; rejection is safe but might block users on newer PG. Policy: range updates с each PG release + `feature = "unverified-pg-version"` escape для early adopters.

---

**End of document.** From here — code.
