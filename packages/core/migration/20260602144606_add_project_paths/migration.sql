CREATE TABLE `project_path` (
	`project_id` text NOT NULL,
	`path` text NOT NULL,
	`type` text NOT NULL,
	`time_created` integer NOT NULL,
	CONSTRAINT `project_path_pk` PRIMARY KEY(`project_id`, `path`),
	CONSTRAINT `fk_project_path_project_id_project_id_fk` FOREIGN KEY (`project_id`) REFERENCES `project`(`id`) ON DELETE CASCADE
);
