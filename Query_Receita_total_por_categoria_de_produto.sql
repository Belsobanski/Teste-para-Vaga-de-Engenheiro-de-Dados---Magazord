SELECT p.id, SUM(t.quantidade * p.preco) AS receita_total
FROM transacoes t
JOIN produtos p ON t.id_produto = p.id
GROUP BY p.id
ORDER BY receita_total DESC;
