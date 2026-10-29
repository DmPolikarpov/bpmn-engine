-- Серверная схема проекта (M6, `erp-simulator/docs/dairy-plant-roadmap.md`, 5).
--
-- До сих пор проект жил только в `localStorage` браузера: вторая сессия (или
-- коллега) видела демо-схему SDK, а версия схемы была известна только тому
-- браузеру, который её правил. Здесь проект переезжает в БД движка: кабинет
-- загружает схему при входе, «Задеплоить» поднимает версию, а второй браузер
-- видит ту же схему и тот же номер версии.
--
-- `project_id` — идентификатор, который выдал кабинет (`project-…`). Он же
-- ключ кэша в браузере, поэтому схема и локальная копия не расходятся.
--
-- `project_versions` хранит историю деплоев: по ней видно, кто и когда менял
-- схему, и можно отдать конкретную версию (`GET …?version=N`). Версия
-- монотонна на проект: `version + 1` считает UPDATE в той же транзакции, что и
-- запись истории, поэтому две одновременные правки не дадут двух «v2».
CREATE TABLE IF NOT EXISTS projects (
    project_id  TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    version     INTEGER NOT NULL DEFAULT 1 CHECK (version > 0),
    bpmn_xml    TEXT NOT NULL,
    updated_by  TEXT NOT NULL DEFAULT '',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_projects_updated_at
    ON projects(updated_at DESC);

CREATE TABLE IF NOT EXISTS project_versions (
    project_id  TEXT NOT NULL,
    version     INTEGER NOT NULL CHECK (version > 0),
    bpmn_xml    TEXT NOT NULL,
    updated_by  TEXT NOT NULL DEFAULT '',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (project_id, version)
);

CREATE INDEX IF NOT EXISTS idx_project_versions_created_at
    ON project_versions(project_id, created_at DESC);
