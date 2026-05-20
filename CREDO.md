# CREDO — железные установки проекта `bsql`

Этот документ — канонический источник принципов разработки. Всё что
противоречит CREDO — **автоматически отвергается**, независимо от
локального удобства, размера изменения или психологической нагрузки.

Любой спор — читаем CREDO. Любой архитектор-ревью, субагент, coding
session, аудит — сверяется с CREDO. Никаких исключений.

---

## §0 Первичная формула

> **Цель — не "лучше чем было" и не "лучше других".**
> **Цель — ЛУЧШЕЕ ИЗ ТЕОРЕТИЧЕСКИ И ГИПОТЕТИЧЕСКИ ВОЗМОЖНОГО.**

Все остальные принципы — следствия из этого.

---

## §1 Приоритетная пирамида

При конфликте решений — побеждает более высокий приоритет. Без
исключений.

1. **ABSOLUTE SAFETY** — отсутствие UB, silent corruption, silent
   data loss, скрытых панических путей, data races, необоснованных
   allocation failures. Железная надёжность. Устойчивость под любой
   нагрузкой и любым вредоносным вводом.
2. **Tier elevation** — всегда повышать tier вверх:
   - **Tier-4 (silent fallback) — запрещён ПОЛНОСТЬЮ.** Любое
     появление = баг-класс, лечится немедленно. Повышаем хоть до
     чего-нибудь > tier-4.
   - **Tier-3 → Tier-2** при технической возможности. Runtime-check
     с явным `Err` — лучше чем silent, но **structural invariant**
     лучше чем runtime-check. Ищем упаковку в bounded container /
     generational ref / typestate.
   - **Tier-2 → Tier-1** при технической возможности. Structural
     runtime — лучше чем classified error, но **compile-time proof**
     лучше чем runtime anything. Ищем const-assert / typed newtype /
     exhaustive match / forbid lint.
   - **Tier-1** — золотой стандарт; любой инвариант закрытый
     compile-time'ом = cost-free at runtime, catches drift at build
     time.
   - **Правило:** при каждой правке пройти по инвариантам, спросить
     "можно ли поднять tier?" — если да, поднимаем. Отказ повышать
     без структурной причины = нарушение §1.
3. **Zero-cost / zero-alloc performance** — нулевая аллокация
   где это архитектурно имеет смысл, агрессивнейшие оптимизации,
   каждая наносекунда на счету, каждый килобайт имеет вес.
   `#![forbid(unsafe_code)]` — универсальная рамка для всего
   проекта (любой крейт). `no_std` / `no_alloc` — **per-crate**,
   применяются там где это соответствует назначению крейта
   (sans-IO protocol cores, embedded-adjacent контексты); user-land
   крейты (driver, CLI, server) могут зависеть от `std` / `alloc`
   когда это обосновано. Решение per-crate принимается явно в
   Cargo.toml + crate-root attribute.
4. **Ergonomics / API surface** — чистый API, понятные типы.
5. **Diff size / session budget** — размер коммита, время сессии.

**Commit size НИКОГДА не побеждает safety, tier elevation, или
zero-cost perf. Время — тем более.**

---

## §2 Дотошность (thoroughness / diligence)

Перед реализацией — **продумать до конца**. Перед отправкой
собственного "готово" — **перепроверить трижды**.

- Любую архитектурную правку рассматривать **под разными углами**:
  что если race? что если split input? что если overflow? что если
  stale ref? что если empty? что если max? что если mid-transition
  drop? что если allocation pressure? что если concurrent reader?
- Все **edge cases** выписать до начала реализации.
- Все **инварианты** пронумеровать, классифицировать по tier'у,
  закрыть соответствующим механизмом.
- Любой `match` — проверить на exhaustive. Любой `if` — на
  пропущенную ветку. Любой `unwrap_or` / `unwrap_or_default` — это
  **silent fallback**, banned без явного обоснования tier'а.
- Лучше подумать глубже в начале, чем ловить панику в конце.

---

## §3 Мнительность (skepticism)

**Первый пришедший вариант — редко лучший.** Искать глубже, дальше,
больше, лучше.

- Перед тем как сказать "готово" — спросить: **можно ли чище?**
  **можно ли эффективнее?** **можно ли безопаснее?** **можно ли
  сдвинуть tier вверх?**
- Любое "само собой разумеющееся" — проверить. Любую "известную
  практику" — проверить в контексте проекта.
- Любое "это же и так оптимально" — измерить или доказать структурно.
- Любой review-комментарий "это можно не делать" — **обосновать
  письменно** почему безопасно не делать. По умолчанию — делаем.

---

## §4 Архитектурные правки приветствуются

- **Большой рефактор — welcome**, если это путь к "best theoretically possible".
- **Breaking changes** — welcome, если project-local или если каскад
  к упрощению > цена миграции.
- **Полная переделка сабсистемы** — welcome, если текущая
  архитектурно ограничена.
- **`#![forbid(unsafe_code)]`** — НЕ торгуется, универсально для
  всего проекта. Любой крейт (sans-IO core, driver, CLI, server,
  macro) остаётся в safe Rust ВСЕГДА. Нет "только для этой
  горячей функции".
- **`no_std` / `no_alloc`** — per-crate, применяются где
  архитектурно оправдано (sans-IO protocol cores, embedded).
  User-land крейты (driver с async, CLI, server) зависят от `std`
  когда это нужно — это нормально. Решение явное в Cargo.toml +
  crate-root attribute.
- Всё остальное — torгуется если даёт структурный win по §1.

---

## §5 Запрещённые отговорки

Эти фразы — **автоматический сигнал что решение принято на
неправильных основаниях**. Услышав их от себя — откатись и
пересмотри.

| Запрещено | Почему |
|-----------|--------|
| "Ой, тесты посыпались, откачу" | Тесты — следствие реализации. Реализация сверяется с CREDO, не с прошлыми тестами. Тесты мигрируются. |
| "Это не bottleneck — отложу" | Любой объективный winning объём — реализовать. Не "только bottleneck'и". |
| "Это слишком сложно — не стоит того" | Сложность — не критерий отказа. Критерий — совпадение с CREDO. |
| "Сделаем в следующей сессии / завтра / потом" | Запрещено. Если достижимо сейчас — делаем сейчас. |
| "Очистить остальное" / "cleanup потом" | Всё в работу за один проход. "Остального" не существует — есть отсортированный по dependency список, реализуемый целиком. |
| "Минимум рисков" | Риск измеряется в **потенциальных компромиссах safety/tier-1**, не в размере diff'а или количестве сломанных тестов. |
| "Это уже достаточно хорошо" | Достаточно = теоретически возможный максимум. До тех пор — **недостаточно**. |
| "Начнём с low-risk чтобы проверить паттерн" | Паттерны проверяются **архитектурно** (в уме, в документе), не через safer-but-smaller shipping. Если уверены в паттерне — сразу cascade. Если нет — сначала **больше думать**, не меньше делать. |
| "Half-measure — потом доделаем" | Half-measure в safety/tier-1 — запрещён. Либо полный tier-1 shield, либо классифицированный tier-2 с ясным audit-trail. |
| "Здесь утечки architecturally быть не может" | Не аргумент. Проверяется **структурой** (arena-gen-ref lifetime cascade, typestate, Drop instrumentation). Слова — не доказательство. §7 ось 11 — memory-leak невидима и страшнее crash'а. |
| "Fallback — это безопаснее чем возвращать error" | Нет. Fallback без классификации = silent corruption = tier-4 = §1 нарушение. Явный `Result::Err(classified)` **всегда** безопаснее чем тихий default. §7 ось 12. |
| "Default value тут правильный fallback" | Проверить: почему именно этот default? Если ответ "потому что не знаю что ещё вернуть" — это **костыль**, не recovery. Лечи invariant выше. |

---

## §6 Аудит-loop discipline

Пользователь будет запускать аудиты **снова и снова** пока качество
не достигнуто. Это — **штатный механизм**, не exception.

- Каждый аудит — смотрит свежим взглядом, ищет blind spots, не
  ограничивается прошлым списком находок.
- Каждая новая находка из любого аудита — **добавляется в очередь
  работ**. Не отвергается потому что "уже отработали тему".
- Чем глубже аудит думает ДО начала реализации — тем меньше
  аудит-циклов нужно. Побочное следствие CREDO.
- Агент-архитектор (architect subagent) — обязан получать
  CREDO в контексте. Запуск audit'а без CREDO — test case,
  мерящий что агент находит без подсказок; но **реальный план** —
  строим с CREDO.
- Skepticism применять и к архитектору: если агент маркирует что-то
  "DONE" — перепроверить самостоятельно. Не доверять словам,
  доверять **аргументу**.

---

## §7 Edge-case discipline

**Принцип:** "Лучше рассмотреть поглубже и понять что это не про
это, чем не рассмотрев потом упереться в эту проблему."

Для каждой non-trivial правки — **обязательно прокрутить в уме
все оси**. Не каждая ось будет применима (например async/параллелизм
не везде), НО:

- Пройти по каждой оси — **обязательно**, не по "релевантным на
  первый взгляд".
- Решение "ось не применима" — **явно обосновать** (комментарий
  в коде / PR-description / deferred.md). "Async не применим т.к.
  функция синхронная по контракту" — ок. **Тишина** типа "просто не
  подумал" — нарушение §7.

### Оси рассмотрения

**1. Cardinality (много/мало/ноль):**
- Пустой вход (empty buffer, zero rows, empty query, 0 columns)
- Один (single frame, single column, single row)
- Типичный (few — 2-10)
- Много (at-capacity, stream density max, MAX_COLUMNS)
- Превышение (capacity+1, MAX+1 overflow, width-overflow)

**2. Presence (одновременно/отсутствует/дублируется):**
- Все ожидаемые поля присутствуют
- Все ожидаемые поля отсутствуют
- Частичная presence (некоторые есть, некоторые нет)
- Дубликат (field прислан дважды — что побеждает?)
- Unexpected presence (что-то пришло чего не ждали)

**3. Concurrency (параллельно/отдельно):**
- Single-threaded sequential (no concurrency) — default для sans-IO
- Multi-threaded shared — требования Send/Sync; есть ли `PhantomData<Cell<()>>` для `!Sync`?
- Async task boundaries — cancellation-safety, Drop-in-flight future, pinning
- Reentrancy — call into self during callback / closure / drop
- Signal / preemption (на embedded target'ах)

**4. Temporal (прервано/задержки/split):**
- Полная последовательность за один вызов (happy path)
- Split через feed() calls — mid-frame, mid-header, byte-by-byte
- Mid-transition drop / panic / cancellation (user future dropped)
- Timeout / stale reference (generation mismatch в arena)
- Reorder (frames пришли в другом порядке — возможно ли?)
- Повторный вызов / повторное потребление (re-entry)

**5. Trust level (доверенный/атакующий):**
- Internal-only (trusted invariants по конструкции)
- Semi-trusted (authenticated server после handshake)
- Untrusted (pre-auth, arbitrary bytes)
- Malformed (wire-valid по форме но semantically wrong)
- Adversarial (specifically crafted для обхода парсера)

**6. Size (нулевой/средний/максимум/переполнение):**
- Zero bytes / empty slice
- Максимум (capacity-1, capacity, capacity+1)
- Integer width overflow (u8/u16/u32 narrowing)
- Variable-width payload с declared length != actual length

**7. State lifecycle (начало/середина/конец):**
- Pre-init / fresh state
- Mid-operation (partial state)
- Terminal (drained / completed)
- Errored recovery path (после fail_inflight)
- Post-drop (use-after-free — Rust предотвращает, но проверить
  arena-gen-ref семантику)

**8. Resource pressure (ресурсы):**
- Bounded buffer slack (есть место)
- At-capacity (bounded full — graceful?)
- Over-capacity (classified refuse, не silent drop)
- Arena slot exhaustion (stale-gen-ref handling)
- Stack pressure (large enum variants, deep recursion)

**9. Platform (endianness/alignment/pointer width):**
- Little-endian vs big-endian (protocol — BE, host — LE обычно)
- Alignment (4/8-byte bounds, `#[repr(C)]` vs `#[repr(Rust)]`)
- `target_pointer_width` (32 vs 64 bit — u16/usize интеракции)
- Platform-specific behaviour (panic=abort vs unwind)

**10. Failure composition (как ошибки складываются):**
- Single error class — один `Result::Err`
- Cascading (ошибка → другая ошибка → третья)
- Партиальная реализация (часть succeeded, часть failed)
- Recovery path (error → recover → retry) — допустим ли?
- Fatal vs recoverable classification — кто тэгает?

**11. Memory-leak / ownership (утечка / владение):**
- Утечка памяти невидима. В long-running connection'е может
  копиться гигабайтами без единого crash'а — страшнее чем panic,
  потому что не сигналит. При **каждой** правке думать: "где тут
  может что-то не освободиться?"
- **Arena slot** — после использования освобождается? Generational
  ref защищает от use-after-free, но от un-free ничего не спасает,
  только дисциплина.
- **Bounded buffer** — при обрыве mid-operation `clear()` вызывается?
  Иначе — "utility" но invisible leak в stale data.
- **Reply-correlator slot** — после delivery / fail освобождается
  чтобы не стакать `ReplyId` бесконечно?
- **Pending-advance / other state bits** — очищаются при Errored
  transition? Иначе — zombie state.
- **Sensitive data** (пароли, SCRAM proofs, session tokens) —
  `zeroize` при drop? Утечка приватных данных в memory dump — CVE-class.
- **FFI-owned pointer** (если появится) — кто owns, кто frees?
- **Thread-local / task-local state** — очищается после task drop?
- **Circular Arc refs** (если `std` появится) — loop'ы detected?
- Borrow-checker проверяет Drop timing, но НЕ проверяет что drop
  действительно пригласился для arena-managed objects. Arena
  invariants — manual.

**12. Fallback / recovery path (fallback-поведение):**
- **Fallback — возможен, но не бездумно.** Fallback сам по себе НЕ
  преступление — classified `Result::Err` есть fallback от
  happy-path, это tier-3, ок. Преступление — **бездумный** /
  **необоснованный** / **silent** fallback. Каждый fallback
  должен иметь:
  - **Классификацию** по tier'у (tier-3 через `Err(classified)` —
    ОК; tier-4 через `unwrap_or` / silent default — запрещён).
  - **Обоснование**: почему именно этот fallback, не другой? Не
    "первый пришедший в голову".
  - **Доказательство что это recovery, а не костыль**: если
    fallback лечит симптом, а реальная проблема в invariant'е
    уровнем выше — лечи invariant, не симптом.
- **Fallback как путь к костылям:** если функция требует длинный
  fallback chain — часто признак что invariant выше по стеку
  сломан и лечится неправильно, а fallback — workaround.
- **Классификация fallback'а по tier'у:**
  - Fallback → `Result::Err(classified)` = tier-3, приемлем.
  - Fallback → `Result::Err(generic "internal error")` = tier-3 но
    слабее; диагностика страдает.
  - Fallback → default value без классификации = **tier-4, silent
    corruption**, запрещён §1.
  - Fallback → panic = tier-3 структурно-максимум но runtime cost +
    ломает "no panics" goal.
- **Вопросы:**
  - Есть ли fallback? Если да — почему?
  - Fallback создаёт костыль — чинит ли он симптом, скрывая
    баг в invariant'е выше?
  - Fallback diverges от happy-path semantics — observable ли
    разница caller'у? Если да — это **расхождение** (поломка
    API-контракта), не recovery.
  - Fallback retry — finite? Infinite-loop possible?
  - Fallback transitions state — preserves ли target-state
    invariants?
  - "Default" value как fallback — а этот default реально correct
    для контекста, или "nothing better to return"?
- **Правило:** если при code review видишь fallback — прежде чем
  принять, спросить "а можно без него?" Часто можно — через
  tier-upgrade (tier-3 → tier-2 через typestate / bounded
  container) или пересмотр invariant'а.

### Применимость

Не всё релевантно всегда. Пример: pg-proto crate какой-нибудь — sync sans-IO,
ось Concurrency (async/parallel) большей частью "не применимо — код
синхронный, `!Sync` gated". Но: Drop-in-flight всё равно применимо
(user future может быть dropped mid-feed_bytes, wrapper должен быть
cancellation-safe). Так что ось **не выкидываем целиком**, а
проходим по подпунктам.

Memory-leak (ось 11) и Fallback (ось 12) — **почти всегда
применимы**. Утечка памяти возможна везде где есть владение
ресурсом; fallback применим везде где есть branching на ошибки. Это
две оси которые особенно важно проходить осознанно, а не "по
умолчанию нет".

### Правило

Для каждой non-trivial правки в PR / коммит / deferred.md
отметить — явно или неявно через assurance — что по всем 12 осям
прошли. Неосмотренная ось = drift surface = latent bug class.

---

## §8 Tier-taxonomy (напоминание + elevation policy)

**Tier-1** — compile-time. Нарушение = build error. Пример: typed
newtype, const-assert, exhaustive match, `forbid` lint.

**Tier-2** — structural runtime. Нарушение ловится при runtime но
через **структуру типов / invariant-проверяемую функцию**. Пример:
bounded buffer capacity, generational arena ref, `NonZero*` niche.

**Tier-3** — классифицированная ошибка. Runtime check с явным
`Result::Err` / classified error enum. Пример: wire-level parse
error, server ErrorResponse.

**Tier-4** — silent fallback. **ЗАПРЕЩЁН**. `unwrap_or(())`,
`if let ... else { /* nothing */ }`, silent `u16::try_from(...)`
с фоллбэком на константу — всё tier-4. Любое появление tier-4 —
багрепорт-class, лечится немедленно.

---

### Elevation policy (ключевое)

**Tier — не статус. Tier — это "куда мы хотим двигать этот
инвариант дальше".**

- **Tier-4 → любой tier > 4.** Приоритет #1. Никакого silent. Хоть
  classified error (tier-3), хоть structural check (tier-2), хоть
  compile-time (tier-1) — только не silent. Любое обнаружение tier-4
  = рабочий ticket в очередь правок, не "сейчас неприоритетно".

- **Tier-3 → Tier-2 когда структурно возможно.** Classified error
  — честно, но runtime-check всё ещё стоит CPU и занимает место в
  code. Если инвариант можно упаковать в bounded container, typed
  newtype с validated constructor, generational ref — делаем.
  Примеры: `ReadBuf.append` raw `Result<(), ReadBufFull>` → уже
  tier-2 (bounded capacity проверяется внутри). `SqlStateCode` как
  `FixedStr<5>` с `from_bytes_truncating` — tier-2 structural. Можно
  бы было поднять до tier-1 const-validated если все значения
  перечисляемы.

- **Tier-2 → Tier-1 когда технически возможно.** Structural runtime
  check всё равно стоит одну инструкцию сравнения. Compile-time
  proof — 0 инструкций runtime. Примеры: `const _ = assert!(...)`
  на размер enum'а. `repr(u8)` + `#[non_exhaustive]` + exhaustive
  match — компилятор ловит недостающие arm'ы. Typed newtype с
  private-field private-constructor — swap через `.0` невозможен.

- **Tier-1 → better tier-1.** Даже внутри tier-1 есть спектр: const
  round-trip pin < exhaustive match discriminant < typed newtype с
  invariant-constructor < sealed trait via pub(crate). Всегда
  выбирать сильнейшую форму.

**Правило дня:** при каждой правке пройти по инвариантам:
1. Есть ли где tier-4? → убрать немедленно.
2. Есть ли tier-3 который можно переупаковать в tier-2? → попробовать.
3. Есть ли tier-2 который можно перенести в tier-1? → попробовать.
4. Есть ли tier-1 который можно сделать более сильным? → подумать.

Отказ повышать tier без структурного обоснования (например, "это
wire-level input, tier-1 невозможно по природе") — **нарушение §1
пункт 2**.

---

## §9 Perf-дисциплина

- **Zero-alloc** — путь по умолчанию. Если аллокация нужна — она
  пересматривается.
- **Zero-copy** — путь по умолчанию. Копии обосновываются.
- **Zero-init** — любое `[T; N]` zero-filled на горячем пути —
  потенциальная находка.
- **Zero-branch** — на горячем пути branch'и минимизировать. LUT,
  bitwise, branchless arithmetic — welcome.
- **Zero-leak** — НИ ОДНОГО пути где ресурс может не освободиться.
  Владение доказывается **структурно** (Drop, arena-gen-ref с
  явным `free`, RAII guards). Long-running connection — тест на
  leak'и обязателен. См. §7 ось 11.
- **Zero-fallback** — fallback-path ≠ recovery. Любой "default
  value" в качестве fallback'а — проверить: это действительно
  recovery, или костыль вокруг сломанного invariant'а уровнем
  выше? См. §7 ось 12.
- **Const propagation** — любое runtime-вычисление значения, которое
  могло бы быть const — flag. Const-fn rollout — welcome.
- **Arena > pool > alloc** — когда есть выбор: arena-with-gen-ref
  всегда предпочтительнее pool'а; pool — alloc'а.
- **Batch > single** — batched write / batched dispatch / batched
  decode — welcome если можно обосновать invariant'ы batch'а.
- **Lazy > eager** — lazy parse / lazy decode — welcome если
  invariant'ы LP'а (borrow lifetime, ownership) не усложняют
  каскад.
- **Static > const > runtime** — то что может быть `static` —
  `static`. То что может быть `const` — `const`.
- **`#[cold]` / `#[inline]` / `#[inline(always)]`** — явные hints
  LLVM на горячих/холодных путях.
- **Bitwise packing** — если variant count ≤ 256, `repr(u8)` +
  packed discriminant — welcome.
- **NonZero niches** — везде где sentinel ≠ 0, использовать
  `NonZero*` для niche optimization.

---

## §10 Application

**При каждом коммите** — commit message обосновывает соответствие
CREDO. Если правка делает tier хуже (tier-1 → tier-2, или
classified → silent) — это **регрессия**, требует явного
обоснования + план возврата.

**При каждом audit-запросе** — архитектор-агент получает ссылку
на CREDO в системном промпте.

**При каждом planning-обсуждении** — порядок работ строится по
CREDO, не по "удобству" или "low-risk first".

**Все находки из всех аудитов — добавляются в очередь работ.**
Отсев допускается ТОЛЬКО если находка:
- (a) доказанно уже закрыта эквивалентом,
- (b) architecturally невозможна без нарушения §1,
- (c) обосновано вне scope проекта (например, про OS kernel, а мы
  пишем user-land lib).

Отсев по критерию "лень", "большой diff", "сложно" — **запрещён
CREDO'м**.

---

## §11 Safe-Rust idioms (не путать с unsafe)

Некоторые конструкции **выглядят** как потенциальные угрозы / слабые
места / костыли, но на самом деле идиоматичны и safe. Фиксирую чтобы
при аудите не тратить время на false-positive.

### `*var = value` — place-assignment через `&mut T`

`*slot = bytes` где `slot: &mut [u8; 4]` — это НЕ pointer
dereference в unsafe-смысле. Это **place expression**: пишем в
место, на которое указывает `&mut T`. Единственный способ
присвоения через mutable reference в safe Rust.

- Для `&mut [u8; N]` (фиксированный массив) — `*slot = value`
  оптимально: type-level size match, без runtime bounds-check.
  LLVM → single machine-word store на aligned targets.
- Для `&mut [u8]` (runtime-sized slice) — `.copy_from_slice(&src)`
  (добавляет проверку `src.len() == dst.len()`).
- Для `&mut T` generic — `*t = value` или `std::mem::replace`.

**Не крутка и не угроза.** Альтернативы для fixed-array:
`copy_from_slice` эквивалентно но теряет type-level size guarantee
(компилятор добавляет ассерт). `*slot = arr` сильнее.

### `*byte` в slice-pattern / closure

```rust
payload.iter().position(|b| *b == 0)  // b: &u8 → *b: u8
```

Deref `&u8` → `u8` для сравнения/копии Copy значения. Zero-cost.
Pattern `[*a, *b, *c, *d]` в `[a, b, c, d, ..]` slice match —
тот же deref для разбивки `&[u8]` на Copy-значения.

### `core::mem::ManuallyDrop<T>` — вводит в заблуждение по имени

Название SOUNDS like "вручную очищать из памяти". Это **неправда**.
Точный смысл:

- `ManuallyDrop<T>` — wrapper, который **отключает** автоматический
  Drop внутреннего T.
- Если T не имеет Drop-логики (Copy type / trivial Drop body),
  skipping Drop полностью безопасен — нечего очищать.
- Если T имеет важную Drop (zeroize / resource release), skipping
  Drop = LEAK. Осторожно.
- Назывался бы правильнее `SkipAutoDrop` или `NoDropOnScopeEnd`.
  Историческое имя Rust.

**В нашем коде** (`OutActions`): `T = heapless::Vec<Action, N>`.
Action is Copy → Vec's Drop body trivial (walks over Copy elements,
no-op each). Skipping Drop = 0 runtime cost, 0 leak risk. Чисто
safe.

Документируется в `lib.rs` const-assert `!needs_drop::<OutActions>`.

### `unwrap_or(&[])` на architecturally-dead branch

`.get(..n).unwrap_or(&[])` — **силайт-корлор tier-4**. Заменять
на explicit match:
```rust
match items.split_at_checked(n) {
    Some((head, _)) => head,
    None => &[],  // классифицированно-мёртвый empty-sentinel
}
```
Либо на `debug_assert!(...)` + documented-dead arm. Пример:
`OutActions::as_slice`.

### `mem::take(&mut t)` + `*t = new`

Pattern для state-machine transitions: временно вынуть значение,
построить новое, записать на место. Safe Rust place-assignment.
Не пугаться `*t = new`.

### `heapless::Vec::new()`

Constructor zero-writes (storage `[MaybeUninit<T>; N]` остается
uninit для незаполненных слотов). Capacity reserved в stack
frame, но байты init не платятся. В отличие от `[T; N]`
literal-fill который eager-init'ит все N слотов.

---

## §12 Ссылки

- `deferred.md` — живой ship log и список defer'ов. Каждый defer
  имеет срок / trigger / причину, соответствующую CREDO.
- Memory (`~/.claude/projects/.../memory/`) — живая память,
  feedback/project/reference/user. Обновляется каждую сессию.
- `TODO` — личный скратчпад пользователя, агенту **читать
  запрещено**.

---

**Последнее обновление:** 2026-04-23 — инициализация при явном
запросе пользователя после 3 сессий (W)/(X)/(Y) рефакторинга
bsql-pg-proto. Повод: несколько раз прозвучало "минимум рисков /
очистить остальное / P0 first" — формулировки, противоречащие §5.
CREDO фиксирует принципы жёстко чтобы ни один будущий драфт/plan
не свалился в такие же конструкции.
