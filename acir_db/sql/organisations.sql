SELECT
                organisations.id AS organisation_id,
                organisations.name AS organisation_name,
                organisations.slug AS organisation_slug,
                organisations.cricos_code AS organisation_cricos_code,
                organisations.rto_code AS organisation_rto_code,
                organisations.TCSI_higher_education_provider_id AS tcsi_provider_id,
                organisations.web_address AS organisation_web_address,
                organisations.description AS organisation_description,
                organisation_types.id AS organisation_type_id,
                organisation_types.name AS organisation_type_name,
                simple_organisation_types.id as simple_organisation_type_id,
                simple_organisation_types.name as simple_organisation_type_name,
                regions.id as region_id,
                regions.name AS region_name,
                regions.name_short AS region_name_short,
                states.name AS state_name,
                states.abbreviation AS state_abbreviation,
                sites.id AS site_id,
                sites.name AS site_name,
                sites.study_area AS site_study_area,
                sites.accommodation AS site_accommodation,
                sites.transport AS site_transport,
                sites.comments AS site_comments,
                sites.primary_site AS site_primary_site,
                sites.street1 AS site_street1,
                sites.street2 AS site_street2,
                sites.suburb AS site_subrub,
                sites.postcode AS site_postcode,
                sites.latitude AS site_latitude,
                sites.longitude AS site_longitude,
                sites.country_id AS site_country_id,
                sites.in_vic_scope AS site_in_vic_scope,
                sites.in_qld_scope AS site_in_qld_scope,
                countries.name AS site_country_name,
                site_types.id AS site_type_id,
                site_types.name AS site_type_name,
                logos.id AS logo_id,
                logos.image AS logo_image,
                logos.image_urls AS logo_image_urls,
                organisation_le.id AS organisation_le_id,
                sectors.id AS sector_id,
                sectors.name AS sector_name,
                school_levels.id AS school_level_id,
                school_levels.name AS school_level_name,
                genders.id AS gender_id,
                genders.name AS gender_name,
                (SELECT
                    GROUP_CONCAT(DISTINCT sq.id SEPARATOR ',')
                FROM
                    course_sites AS cs
                    JOIN courses AS c ON (c.id = cs.course_id
                            AND cs.site_id = sites.id
                            AND cs.deleted_at IS NULL
                            AND c.deleted_at IS NULL)
                    JOIN qualifications AS q ON (q.id = c.qualification_id)
                    JOIN qualification_simple_qualification AS qsq ON (qsq.qualification_id = q.id)
                    JOIN simple_qualifications AS sq ON (sq.id = qsq.simple_qualification_id)
                WHERE
                    cs.deleted_at IS NULL
                    AND cs.site_id = sites.id) AS sq_ids,
                (SELECT
					GROUP_CONCAT(DISTINCT fos.id SEPARATOR ',')
				FROM
					course_sites AS cs
					JOIN courses AS c ON (c.id = cs.course_id
							AND cs.site_id = sites.id
							AND cs.deleted_at IS NULL
							AND c.deleted_at IS NULL)
					JOIN course_field_of_study AS cfos ON (cfos.course_id = c.id)
					JOIN field_of_studies AS fos ON (fos.id = cfos.field_of_study_id)
				WHERE
					cs.deleted_at IS NULL
					AND cs.site_id = sites.id) AS fos_ids,
                (SELECT
                    1
                FROM
                    course_sites
                    JOIN sites ON (sites.id = course_sites.site_id
                            AND sites.deleted_at IS NULL)
                    JOIN courses AS c ON (
                        c.id = course_sites.course_id AND c.deleted_at IS NULL
                    )
                WHERE
                    sites.organisation_id = organisations.id
                    AND course_sites.cricos IS NOT NULL
                    AND course_sites.deleted_at IS NULL
                    AND course_sites.archived_at IS NULL
                LIMIT 1) AS has_accredited_courses,
                EXISTS (
                    SELECT 1 FROM course_sites cs
                        JOIN courses AS c ON c.id = cs.course_id
                        JOIN sites s ON s.id = cs.site_id
                    WHERE
                        s.organisation_id = organisations.id
                    AND c.deleted_at IS NULL and cs.deleted_at is null
                    AND s.deleted_at IS NULL
                ) AS has_active_courses,
                EXISTS(SELECT 1 FROM app_organisation_client_profile aocp INNER JOIN client_profiles cp ON aocp.client_profile_id = cp.id
                    WHERE aocp.app_id = 1 AND cp.active = 1 AND aocp.organisation_id = organisations.id LIMIT 1) AS is_active_gug_client,
                EXISTS(SELECT 1 FROM app_organisation_client_profile aocp INNER JOIN client_profiles cp ON aocp.client_profile_id = cp.id
                    WHERE aocp.app_id = 2 AND cp.active = 1 AND aocp.organisation_id = organisations.id LIMIT 1) AS is_active_gsg_client,
                EXISTS(SELECT 1 FROM app_organisation_client_profile aocp INNER JOIN client_profiles cp ON aocp.client_profile_id = cp.id
                    WHERE aocp.app_id = 3 AND cp.active = 1 AND aocp.organisation_id = organisations.id LIMIT 1) AS is_active_sia_client   
            FROM
                organisations
                LEFT JOIN organisation_types ON organisation_types.id = organisations.organisation_type_id
                LEFT JOIN organisation_type_simple_organisation_type ON organisation_type_simple_organisation_type.organisation_type_id = organisation_types.id
                LEFT JOIN simple_organisation_types ON simple_organisation_types.id = organisation_type_simple_organisation_type.simple_organisation_type_id
                
                LEFT JOIN sites ON (sites.organisation_id = organisations.id
			        AND sites.deleted_at IS NULL)
                LEFT JOIN countries ON (countries.id = sites.country_id  AND countries.deleted_at IS NULL)
                LEFT JOIN organisation_le ON organisation_le.organisation_id = organisations.id
                LEFT JOIN sectors ON sectors.id = organisation_le.sector_id
                LEFT JOIN school_levels ON school_levels.id = organisation_le.school_level_id
                LEFT JOIN genders ON genders.id = organisation_le.gender_id
                LEFT JOIN site_types ON site_types.id = sites.site_type_id
                LEFT JOIN regions ON regions.id = sites.region_id
                LEFT JOIN states ON states.id = sites.state_id
                LEFT JOIN logos ON (logos.organisation_id = organisations.id AND logos.deleted_at IS NULL)
            WHERE
                organisations.deleted_at IS NULL 
                    AND (organisation_le.id IS NOT NULL
                        OR EXISTS (
                            SELECT
                                1 FROM course_sites
                                JOIN courses AS c ON (c.id = course_sites.course_id
                                        AND c.deleted_at IS NULL)
                            WHERE
                                course_sites.site_id = sites.id))
            ORDER BY organisations.id ASC