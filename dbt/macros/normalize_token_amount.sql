{% macro normalize_token_amount(raw_column, decimals_column) -%}
try_to_decimal(
    case
        when {{ raw_column }} is null or {{ decimals_column }} is null then null
        when {{ decimals_column }} < 0 or {{ decimals_column }} > 18 then null
        when not regexp_like({{ raw_column }}, '^-?[0-9]+$') then null
        when {{ decimals_column }} = 0 then {{ raw_column }}
        else
            iff(left({{ raw_column }}, 1) = '-', '-', '')
            || case
                when length(ltrim({{ raw_column }}, '-')) <= {{ decimals_column }}
                    then '0.'
                        || repeat(
                            '0',
                            {{ decimals_column }}
                            - length(ltrim({{ raw_column }}, '-'))
                        )
                        || ltrim({{ raw_column }}, '-')
                else
                    left(
                        ltrim({{ raw_column }}, '-'),
                        length(ltrim({{ raw_column }}, '-')) - {{ decimals_column }}
                    )
                    || '.'
                    || right(ltrim({{ raw_column }}, '-'), {{ decimals_column }})
            end
    end,
    38,
    18
)
{%- endmacro %}
