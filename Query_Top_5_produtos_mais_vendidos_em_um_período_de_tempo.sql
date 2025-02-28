SELECT p.nome, SUM(t.quantidade) AS total_vendido
FROM transacoes t
JOIN produtos p ON t.id_produto = p.id
WHERE t.data_transacao BETWEEN '2024-01-01' AND '2024-12-31' 
GROUP BY p.nome
ORDER BY total_vendido DESC
LIMIT 5;
