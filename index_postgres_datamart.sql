CREATE INDEX idx_clientes_id ON clientes(id);
CREATE INDEX idx_clientes_nome_sobrenome ON clientes(nome, sobrenome);
CREATE INDEX idx_produtos_id ON produtos(id);
CREATE INDEX idx_produtos_descricao ON produtos(descricao);
CREATE INDEX idx_transacoes_id_cliente ON transacoes(id_cliente);
CREATE INDEX idx_transacoes_id_produto ON transacoes(id_produto);
CREATE INDEX idx_transacoes_data_transacao ON transacoes(data_transacao);
CREATE INDEX idx_clientes_id ON clientes(id);
CREATE INDEX idx_clientes_nome_sobrenome ON clientes(nome, sobrenome);





