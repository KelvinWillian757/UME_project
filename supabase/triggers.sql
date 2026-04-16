create extension if not exists pg_net;

create or replace function public.notify_credit_event()
returns trigger as $$
begin
  perform net.http_post(
    url := '',
    headers := jsonb_build_object('Content-Type', 'application/json'),
    body := jsonb_build_object(
      'event_id', NEW.event_id,
      'entity_id', NEW.entity_id,
      'event_type', NEW.event_type,
      'event_timestamp', NEW.event_timestamp,
      'payload', NEW.payload
    )
  );
  return NEW;
end;
$$ language plpgsql;

drop trigger if exists trg_credit_events on public.credit_evaluations_events;

create trigger trg_credit_events
after insert on public.credit_evaluations_events
for each row
execute function public.notify_credit_event();
