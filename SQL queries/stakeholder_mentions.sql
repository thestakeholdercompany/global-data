WITH content_restriction AS
  (SELECT *
   FROM EXTERNAL_QUERY('projects/genfive-prod/locations/asia-southeast1/connections/alloydb_media', '''
SELECT languages,
        industry_ids,
        geographies,
        blocked_domains,
        stakeholder_ids,
        ARRAY(select cast(UNNEST(types) AS varchar(20))) as media_types
FROM media_focus
WHERE project_id = '[YOUR_PROJECT_ID]'
'''))
SELECT stakeholders.name,
       COUNT(media.id) AS Number_of_Media_Mentions,
       'https://genie.tsc.ai/organizations/57/workspaces/111/stakeholders/' || stakeholders.id AS genie_url
FROM media_dataset.media
CROSS JOIN UNNEST (stakeholder_ids) AS stakeholder_id
JOIN stakeholders_dataset.stakeholders ON stakeholder_id = stakeholders.id
AND stakeholders.is_visible = TRUE,
content_restriction
WHERE published_at BETWEEN '[YOUR_MEDIA_START_DATE]' AND '[YOUR_MEDIA_END_DATE]'
-- organization specific media integrations (!! important !!)
  AND (ARRAY_LENGTH(target_organization_ids) = 0
       OR 57 IN UNNEST(target_organization_ids))
-- media focus filters
  AND (ARRAY_LENGTH(content_restriction.industry_ids) = 0
       OR EXISTS
         (SELECT 1
          FROM unnest(content_restriction.industry_ids) AS industry
          WHERE industry IN unnest(media.industry_ids)))
  AND (ARRAY_LENGTH(content_restriction.geographies) = 0
       OR EXISTS
         (SELECT 1
          FROM unnest(content_restriction.geographies) AS geography
          WHERE geography IN unnest(media.country_ids)
            OR geography IN unnest(media.subcountry_ids)))
  AND (ARRAY_LENGTH(content_restriction.stakeholder_ids) = 0
       OR EXISTS
         (SELECT 1
          FROM unnest(content_restriction.stakeholder_ids) AS stakeholder
          WHERE stakeholder IN unnest(media.stakeholder_ids)))
  AND (ARRAY_LENGTH(content_restriction.languages) = 0
       OR media.language IN unnest(content_restriction.languages))
  AND (ARRAY_LENGTH(content_restriction.media_types) = 0
       OR media.type IN unnest(content_restriction.media_types))
  AND media.publisher NOT IN unnest(content_restriction.blocked_domains) 
  -- Additional filters as requested
  AND lower(content) like '%[YOUR_KEYWORD]%'
  AND [YOUR_STAKEHOLDER_CATEGORY_ID] IN UNNEST(cube_category_ids)
GROUP BY stakeholders.name,
         genie_url
ORDER BY Number_of_Media_Mentions DESC,
         stakeholders.name