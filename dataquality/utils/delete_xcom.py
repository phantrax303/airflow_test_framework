import logging
from airflow.models import XCom
from airflow.utils.session import provide_session

@provide_session
def delete_xcom_function(dag_id: str, run_id: str = None, session=None):
    """
    Versão Híbrida: 
    - Se run_id for passado: Limpa apenas a execução atual e propaga erro (Novo modelo).
    - Se run_id for None: Limpa todos os XComs da DAG (Modelo legado).
    """
    # 1. Analisar falhas apenas se run_id for fornecido (Escopo de propagação)
    failed_count = 0
    failed_ids = []
    
    if run_id:
        from airflow.models import TaskInstance
        failed_tis = session.query(TaskInstance).filter(
            TaskInstance.dag_id == dag_id,
            TaskInstance.run_id == run_id,
            TaskInstance.state == 'failed'
        ).all()
        
        failed_count = len(failed_tis)
        failed_ids = [ti.task_id for ti in failed_tis]

    # 2. Deletar XComs
    query = session.query(XCom).filter(XCom.dag_id == dag_id)
    
    # Se run_id existe, limpamos só esta execução. 
    # Se for None, a query permanece filtrando apenas dag_id (comportamento antigo).
    if run_id:
        query = query.filter(XCom.run_id == run_id)

    deleted_count = query.delete(synchronize_session=False)
    session.commit()
    
    logging.info(f"Limpeza concluída: {deleted_count} XComs removidos para {dag_id} (run_id: {run_id}).")

    # 3. Propagação de erro (Apenas para o novo modelo)
    if run_id and failed_count > 0:
        from airflow.exceptions import AirflowFailException
        logging.warning(f"Propagando falha para as tasks: {failed_ids}")
        raise AirflowFailException(
            f"Limpeza feita, mas a DAG falhou nas tasks: {failed_ids}"
        )
