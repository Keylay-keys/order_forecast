-- Narrow exception to immutable email originals, requested for matched advice.
-- No runtime UPDATE/DELETE grant on originals. Only an active, leased email job
-- with persisted exact-match facts can replace its own retained MIME copy.
CREATE OR REPLACE FUNCTION public.accounting_discard_matched_advice(
    p_message uuid, p_uid text, p_route text, p_lease uuid,
    p_content bytea, p_result jsonb
) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog
AS $function$
DECLARE
    prior_result jsonb;
    prior_content bytea;
    delivery_digest text;
    removed_before jsonb;
    removed_after jsonb;
    document text;
BEGIN
    SELECT j.result, o.content, o.sha256
      INTO prior_result, prior_content, delivery_digest
      FROM public.accounting_email_jobs j
      JOIN public.accounting_email_originals o USING (message_id)
     WHERE j.message_id=p_message AND o.firebase_uid=p_uid AND o.route_number=p_route
       AND j.status='reading' AND j.lease_token=p_lease AND j.lease_expires_at>now()
     FOR UPDATE OF j, o;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Email retention requires the active scoped lease';
    END IF;
    removed_before := coalesce(prior_result->'paymentAdviceRemovedDocuments','[]'::jsonb);
    removed_after := p_result->'paymentAdviceRemovedDocuments';
    IF p_content IS NULL OR p_result IS NULL
       OR octet_length(p_content) NOT BETWEEN 1 AND 26214400
       OR octet_length(p_content)>=octet_length(prior_content)
       OR jsonb_typeof(p_result) IS DISTINCT FROM 'object'
       OR jsonb_typeof(removed_after) IS DISTINCT FROM 'array'
       OR jsonb_typeof(prior_result->'paymentAdviceFiles') IS DISTINCT FROM 'array'
       OR prior_result->'saved' IS DISTINCT FROM 'true'::jsonb
       OR p_result->'paymentAdviceFiles' IS DISTINCT FROM prior_result->'paymentAdviceFiles'
       OR p_result->>'originalContentBytes' IS DISTINCT FROM coalesce(prior_result->>'originalContentBytes',octet_length(prior_content)::text)
       OR NOT (removed_before <@ removed_after) OR removed_before=removed_after
       OR encode(sha256(prior_content),'hex') IS DISTINCT FROM coalesce(prior_result->>'retainedContentSha256',delivery_digest)
       OR p_result->>'retainedContentSha256' IS DISTINCT FROM encode(sha256(p_content),'hex')
    THEN
        RAISE EXCEPTION 'Email retention integrity check failed';
    END IF;
    FOR document IN SELECT jsonb_array_elements_text(removed_after) LOOP
        IF NOT (removed_before ? document) THEN
            IF NOT EXISTS (
                SELECT 1 FROM jsonb_array_elements(prior_result->'paymentAdviceFiles') f
                 WHERE f->>'documentNumber'=document
            ) OR NOT EXISTS (
                SELECT 1 FROM public.accounting_documents d
                 WHERE d.firebase_uid=p_uid AND d.route_number=p_route
                   AND d.document_key='payment-advice:' || document
                   AND d.payload->>'route'=p_route
                   AND d.payload->>'documentNumber'=document
                   AND d.payload->>'status' IN ('recorded','already_recorded')
            ) THEN
                RAISE EXCEPTION 'Only exactly matched payment advice can be discarded';
            END IF;
        END IF;
    END LOOP;
    UPDATE public.accounting_email_originals SET content=p_content
     WHERE message_id=p_message AND firebase_uid=p_uid AND route_number=p_route;
    UPDATE public.accounting_email_jobs SET result=p_result WHERE message_id=p_message;
END
$function$;
REVOKE ALL ON FUNCTION public.accounting_discard_matched_advice(uuid,text,text,uuid,bytea,jsonb) FROM PUBLIC;
