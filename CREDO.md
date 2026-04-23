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
3. **Zero-cost / zero-alloc performance** — нулевая аллокация, агрессивнейшие
   оптимизации, каждая наносекунда на счету, каждый килобайт имеет
   вес. `no_std + no_alloc + #![forbid(unsafe_code)]` — жёсткая
   рамка.
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
- **`no_std + forbid(unsafe_code)`** — не торгуется. В этой рамке
  остаёмся ВСЕГДА. Всё остальное — torguется если даёт структурный
  win.

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

Для каждой правки — пройти check-list:

- [ ] Пустой вход (empty buffer, zero rows, empty query)
- [ ] Максимальный вход (capacity-1, capacity, capacity+1)
- [ ] Split вход (split across feed() calls, split mid-frame,
      split mid-header)
- [ ] Malformed (garbage bytes, length mismatch, missing
      terminator, wrong tag)
- [ ] Stale reference (arena gen mismatch, post-free access)
- [ ] Concurrent access (if applicable)
- [ ] Drop mid-operation (panic safety, reply cancellation)
- [ ] Allocation pressure (bounded buffer full, read-buf full)
- [ ] Integer overflow / narrowing (checked arithmetic, const-assert
      caps)
- [ ] Platform (endianness, alignment, target_pointer_width)

Не один отсутствующий пункт — **not "по умолчанию ок"**. Надо
**обосновать** почему конкретно тут этот edge case исключён.

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

## §11 Ссылки

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
