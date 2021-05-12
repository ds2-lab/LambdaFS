ALTER TABLE yarn_projects_daily_cost ADD COLUMN `app_ids` VARCHAR(3000) NOT NULL DEFAULT '';
alter table yarn_applicationstate MODIFY `appuser` VARCHAR(100);
