select doc.hs_contact_id,
       doc.meeting_id,
       case
           when meet.meeting_internal_notes is null then null
           when position('Meeting type:' in meet.meeting_internal_notes) = 0 then null
           when (
                    case
                        when position('<br>Purpose' in meet.meeting_internal_notes) > 0
                            then position('<br>Purpose' in meet.meeting_internal_notes)
                        when position('<br>Purporse' in meet.meeting_internal_notes) > 0
                            then position('<br>Purporse' in meet.meeting_internal_notes)
                        else 0
                        end
                    ) = 0 then null
           else trim(
                   substring(
                           meet.meeting_internal_notes
                           from position('Meeting type:' in meet.meeting_internal_notes) + length('Meeting type:')
                           for (
                                   case
                                       when position('<br>Purpose' in meet.meeting_internal_notes) > 0
                                           then position('<br>Purpose' in meet.meeting_internal_notes)
                                       when position('<br>Purporse' in meet.meeting_internal_notes) > 0
                                           then position('<br>Purporse' in meet.meeting_internal_notes)
                                       end
                                   )
                               - (position('Meeting type:' in meet.meeting_internal_notes) + length('Meeting type:'))
                   )
                )
           end                     as meeting_calendar,

       meet.meeting_internal_notes as notes,
       doc.country,
       doc.segment,
       doc.spec_type,
       doc.meeting_status,
       doc.scheduling_date,
       doc.meeting_date,
       doc.scheduled_date,
       doc.mql_id,
       doc.open_deal_id,
       doc.deal_id
from test_bi.dp_meet_export_data doc
         left join dw.hs_engagement_live meet
                   on doc.meeting_id = meet.hubspot_id
where created_at >= '2026-01-01'
limit 10

