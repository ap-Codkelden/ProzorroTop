BEGIN TRANSACTION;
DROP TABLE IF EXISTS "procdist";
CREATE TABLE IF NOT EXISTS "procdist" (
    "procedure" TEXT,
    "procedure_name"    TEXT
);
INSERT INTO "procdist" ("procedure","procedure_name") VALUES 
    ('aboveThreshold', 'Відкриті торги з особливостями'),
    ('aboveThresholdUA','Відкриті торги'),
    ('aboveThresholdEU','Відкриті торги (EU)'),
    ('belowThreshold','Спрощена закупівля'),
    ('negotiation','Переговорна процедура'),
    ('negotiation.quick','Переговорна процедура (за нагальною потребою)'),
    ('reporting','Звіт про договір'),
    ('esco','Закупівля енергосервісу'),
    ('competitiveDialogue','Конкурентний діалог'),
    ('competitiveOrdering', 'Конкурентна заявка'),
    ('competitiveDialogueUA','Конкурентний діалог (UA)'),
    ('competitiveDialogueEU','Конкурентний діалог (EU)'),
    ('priceQuotation','Запит ціни пропозицій'),
    ('competitiveDialogue.stage2','Конкурентний діалог, 2 етап'),
    ('competitiveDialogueEU.stage2','Конкурентний діалог (EU), 2 етап'),
    ('competitiveDialogueUA.stage2','Конкурентний діалог (UA), 2 етап'),
    ('aboveThresholdUA.defense','Закупівля для потреб оборони'),
    ('closeFrameworkAgreementUA','Укладання рамкової угоди'), 
    ('simple.defense', 'Спрощені торги для гарантованого забезпечення потреб безпеки і оборони'),
    ('closeFrameworkAgreementSelectionUA','Відбір для закупівлі за рамковою угодою');
COMMIT;
