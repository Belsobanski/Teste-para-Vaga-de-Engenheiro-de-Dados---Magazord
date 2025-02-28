SELECT 
    COUNT(DISTINCT t.id_cliente) AS clientes_ativos
FROM 
    transacoes t
WHERE 
    t.data_transacao >= CURRENT_DATE - INTERVAL '3 months';
