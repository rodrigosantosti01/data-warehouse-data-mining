SELECT 
    nome_orgao,
    ROUND(SUM(valor_orcado), 2) valor_orcado,
    ROUND(SUM(valor_liquidado), 2),
    ROUND(IFNULL(SUM(valor_liquidado) / SUM(valor_orcado),0),2) percentual
FROM fato_despesa a INNER JOIN instituicao b ON a.instituicao_id = b.id
GROUP BY b.nome_orgao;