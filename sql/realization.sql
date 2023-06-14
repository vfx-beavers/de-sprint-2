-- сброс мб существующих таблиц
drop table if exists public.shipping_agreement;
drop table if exists public.shipping_transfer;
drop table if exists public.shipping_status;
drop table if exists public.shipping_info;
drop table if exists public.shipping_country_rates;

-- Создаем таблицу public.shipping_country_rates с полями
CREATE TABLE public.shipping_country_rates(
	id SERIAL NOT NULL,
	country TEXT,
	base_rate NUMERIC(14,3),
	PRIMARY KEY (id)
);

-- Заполняем пустую таблицу public.shipping_country_rates данными
INSERT INTO public.shipping_country_rates (country, base_rate)
select DISTINCT 
shipping_country, shipping_country_base_rate
FROM public.shipping s;

-- Создаем таблицу public.shipping_agreement с полями
CREATE TABLE public.shipping_agreement(
	agreement_id int NOT NULL,
	agreement_number TEXT,
	agreement_rate numeric(14,3),
	agreement_commission numeric(14,3),
	PRIMARY KEY (agreement_id)
);

-- Заполняем пустую таблицу public.shipping_agreement данными
INSERT INTO public.shipping_agreement (agreement_id, agreement_number, agreement_rate, agreement_commission)
SELECT 
	DISTINCT
	(regexp_split_to_array(vendor_agreement_description, ':'))[1]::int as agreement_id,
	(regexp_split_to_array(vendor_agreement_description, ':'))[2]::text as agreement_number,
	(regexp_split_to_array(vendor_agreement_description, ':'))[3]::NUMERIC(14,3) as agreement_rate,
	(regexp_split_to_array(vendor_agreement_description, ':'))[4]::NUMERIC(14,3)as agreement_commission
FROM public.shipping s 
ORDER BY 1;

-- Создаем таблицу public.shipping_transfer с полями
CREATE TABLE public.shipping_transfer(
	id serial NOT NULL,
	transfer_type TEXT,
	transfer_model TEXT,
	shipping_transfer_rate numeric(14,3),
	PRIMARY KEY (id)
);

-- Заполняем пустую таблицу public.shipping_transfer данными
INSERT INTO public.shipping_transfer(transfer_type,transfer_model,shipping_transfer_rate)
SELECT 
	DISTINCT 
	(regexp_split_to_array(shipping_transfer_description, ':'))[1] as transfer_type,
	(regexp_split_to_array(shipping_transfer_description, ':'))[2] as transfer_model,
	shipping_transfer_rate
FROM public.shipping s
ORDER BY 1;

-- Создаем таблицу public.shipping_info с полями
CREATE TABLE public.shipping_info(
	shipping_id int8 NOT NULL,
	vendor_id int8,
	payment_amount numeric(14,2),
	shipping_plan_datetime timestamp,
	transfer_type_id int8, 
	shipping_country_id int8,
	agreement_id int8,
	primary key (shipping_id),
	FOREIGN KEY (transfer_type_id) REFERENCES public.shipping_transfer(id) ON UPDATE CASCADE,
	FOREIGN KEY (shipping_country_id) REFERENCES public.shipping_country_rates(id) ON UPDATE CASCADE,
	FOREIGN KEY (agreement_id) REFERENCES public.shipping_agreement(agreement_id) ON UPDATE CASCADE
);

-- Заполняем пустую таблицу public.shipping_info данными
INSERT INTO public.shipping_info(shipping_id, 
vendor_id, 
payment_amount, 
shipping_plan_datetime, 
transfer_type_id, 
shipping_country_id, 
agreement_id)
SELECT 
	DISTINCT 
	shippingid as shipping_id,
	vendorid as vendor_id,
	payment_amount,
	shipping_plan_datetime, 
	ship_tr.id as transfer_type_id, 
	ship_cr.id as shipping_country_id,
	agreement_id 
FROM public.shipping s
LEFT JOIN public.shipping_transfer ship_tr 
	ON (regexp_split_to_array(s.shipping_transfer_description, ':'))[1] = ship_tr.transfer_type 
	AND (regexp_split_to_array(s.shipping_transfer_description, ':'))[2] = ship_tr.transfer_model
LEFT JOIN public.shipping_country_rates ship_cr
	ON s.shipping_country = ship_cr.country
LEFT JOIN public.shipping_agreement ship_ag
	ON (regexp_split_to_array(vendor_agreement_description, ':'))[1]::int8 = ship_ag.agreement_id 
ORDER BY shipping_id;

-- Создаем таблицу public.shipping_status с полями
CREATE TABLE public.shipping_status(
	shipping_id int8 NOT NULL,
	status TEXT,
	state TEXT,
	shipping_start_fact_datetime timestamp,
	shipping_end_fact_datetime timestamp
);

-- последний статус каждого shipping_id
WITH ship_last_states AS(
SELECT 
	s.shippingid as shipping_id, 
	s.status,
	s.state,
	s.state_datetime,
	ROW_NUMBER() over(PARTITION BY s.shippingid ORDER BY s.state_datetime desc) AS state_order_desc 
FROM public.shipping s
ORDER BY s.shippingid),
-- получаем дату старта доставки
ship_booked_state_datetime AS (
SELECT distinct s.shippingid as shipping_id, s.state_datetime 
FROM public.shipping s
WHERE s.state = 'booked'),
-- получаем дату окончания доставки
ship_recieved_state_datetime AS (
SELECT DISTINCT s.shippingid as shipping_id, s.state_datetime 
FROM public.shipping s
WHERE s.state = 'recieved')
-- Заполняем пустую таблицу public.shipping_status данными
INSERT INTO public.shipping_status
SELECT 
	sls.shipping_id as shipping_id, 
	status,
	state, 
	sbsd.state_datetime as shipping_start_fact_datetime,
	srsd.state_datetime as shipping_end_fact_datetime
FROM ship_last_states sls
LEFT JOIN ship_booked_state_datetime sbsd ON sls.shipping_id = sbsd.shipping_id 
LEFT JOIN ship_recieved_state_datetime srsd ON sls.shipping_id = srsd.shipping_id 
WHERE state_order_desc = 1
ORDER BY sls.shipping_id;

-- создание представления
DROP VIEW IF EXISTS public.shipping_datamart;
CREATE VIEW public.shipping_datamart AS
SELECT 
	si.shipping_id,
	vendor_id,
	st.transfer_type,
	date_part('day',ss.shipping_end_fact_datetime - ss.shipping_start_fact_datetime) AS full_day_at_shipping,
	CASE 
		WHEN ss.shipping_end_fact_datetime > si.shipping_plan_datetime THEN TRUE
		ELSE FALSE
	END AS is_delay,
	CASE 
		WHEN ss.status = 'finished' THEN TRUE
		ELSE FALSE
	END AS is_shipping_finish,
	CASE 
		WHEN ss.shipping_end_fact_datetime > shipping_plan_datetime 
			THEN date_part('day',shipping_end_fact_datetime - shipping_plan_datetime)
		ELSE 0
	END AS delay_day_at_shipping,
	si.payment_amount,
	payment_amount * (base_rate + agreement_rate + shipping_transfer_rate) AS vat,
	payment_amount * agreement_commission AS profit
FROM public.shipping_info si 
JOIN shipping_transfer st ON st.id = si.transfer_type_id  
JOIN shipping_status ss ON ss.shipping_id = si.shipping_id 
JOIN shipping_agreement sa ON sa.agreement_id = si.agreement_id 
JOIN shipping_country_rates scr ON scr.id = si.shipping_country_id 
ORDER BY si.shipping_id
;
