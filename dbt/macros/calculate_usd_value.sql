{% macro calculate_usd_value(amount_expression, price_expression) -%}
    try_to_decimal(
        to_varchar(
            round(
                try_to_decimal(to_varchar({{ amount_expression }}), 28, 18)
                * try_to_decimal(to_varchar({{ price_expression }}), 28, 18),
                8
            )
        ),
        38,
        8
    )
{%- endmacro %}
