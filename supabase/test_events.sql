
#insercao correta

insert into public.credit_evaluations_events (
    entity_id,
    event_type,
    payload,
    processed
)
values (
    'teste-full-pipeline',
    'INSERT',
    jsonb_build_object(
        'id', 'teste-full-pipeline',
        'document', '44444444444',
        'issuer_id', '1',
        'status', 'APPROVED',
        'creation_timestamp', now()::text,
        'update_timestamp', now()::text,
        'evaluation_request', jsonb_build_object(
            'borrower', jsonb_build_object(
                'cpf', '44444444444',
                'id', '400'
            ),
            'store', jsonb_build_object(
                'id', '40',
                'address', jsonb_build_object(
                    'city', 'Belo Horizonte',
                    'federativeUnit', 'MG'
                )
            ),
            'retailer', jsonb_build_object(
                'id', '500',
                'issuerId', '1',
                'categories', jsonb_build_array(
                    jsonb_build_object('id', '21')
                )
            )
        ),
        'evaluation_response', jsonb_build_object(
            'decisions', jsonb_build_object(
                'bvs', jsonb_build_object(
                    'status', 'APPROVED',
                    'reason', 'SCORE_ABOVE_THRESHOLD',
                    'data', jsonb_build_object(
                        'score', 910,
                        'timestamp', now()::text,
                        'acertaCompletoPositivo', jsonb_build_object(
                            'decisao', jsonb_build_object(
                                'aprova', 'S',
                                'score', '0910',
                                'texto', 'NEGOCIACAO RECOMENDADA'
                            )
                        )
                    )
                ),
                'conventionalApplication', jsonb_build_object(
                    'data', jsonb_build_object(
                        'score', 0.81
                    )
                )
            ),
            'startTimestamp', now()::text,
            'endTimestamp', now()::text
        ),
        'datastream_metadata', jsonb_build_object(
            'uuid', gen_random_uuid()::text,
            'source_timestamp', '1775133333000'
        )
    ),
    false
);


#insercao de error_message
insert into public.credit_evaluations_events (
    entity_id,
    event_type,
    payload,
    processed
)
values (
    'teste-quarantine-1',
    'INSERT',
    jsonb_build_object(
        'id', 'teste-quarantine-1',
        'issuer_id', '1',
        'status', 'APPROVED'
    ),
    false
);