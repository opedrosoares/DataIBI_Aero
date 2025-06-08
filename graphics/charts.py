import io
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import os

def gerar_grafico_market_share(share_data, logo_path=None):
    """Gera um gráfico de pizza para visualização do market share"""
    try:
        from utils.constants import operador_icao_para_nome
        labels = [operador_icao_para_nome.get(item['NR_AERONAVE_OPERADOR'], item['NR_AERONAVE_OPERADOR']) for item in share_data]
        sizes = [item['PaxShare'] for item in share_data]
        colors = plt.cm.Paired(range(len(labels)))
        fig, ax = plt.subplots(figsize=(10, 6))
        
        # Adiciona a marca d'água PRIMEIRO
        if logo_path and os.path.exists(logo_path):
            logo_img = plt.imread(logo_path)
            # Posição e tamanho da marca d'água no canto inferior direito
            logo_ax = fig.add_axes([0.65, 0.05, 0.3, 0.3], anchor='SE', zorder=0)
            logo_ax.imshow(logo_img)
            logo_ax.axis('off')  # Esconde os eixos da imagem
            logo_ax.patch.set_alpha(0.0) # Torna o fundo do eixo transparente
            logo_ax.images[0].set_alpha(0.3) # Define a transparência da imagem

        wedges, texts, autotexts = ax.pie(
            sizes, 
            labels=None,
            autopct='%1.1f%%', 
            startangle=140, 
            radius=0.8,
            pctdistance=0.85,
            colors=colors,
            wedgeprops=dict(width=0.4, edgecolor='w'),
            shadow=True
        )
        ax.axis('equal')
        ax.legend(wedges, labels,
                  title="Operadores",
                  loc="center left",
                  bbox_to_anchor=(1, 0, 0.5, 1))
        plt.setp(autotexts, size=8, weight="bold", color="white")
        ax.set_title("Participação de Mercado por Passageiros", pad=20)
        
        # Define o fundo do eixo como transparente para ver a marca d'água
        ax.patch.set_alpha(0.0)

        buf = io.BytesIO()
        plt.savefig(buf, format='png', bbox_inches='tight', transparent=True)
        buf.seek(0)
        return buf
    except Exception as e:
        print(f"Erro ao gerar gráfico de market share: {e}")
        return None

def gerar_grafico_historico(df_historico, tipo_consulta, local, logo_path=None):
    """Gera um gráfico de linha para visualização do histórico de movimentação"""
    try:
        fig, ax = plt.subplots(figsize=(10, 6))
        
        # Adiciona a marca d'água PRIMEIRO
        if logo_path and os.path.exists(logo_path):
            logo_img = plt.imread(logo_path)
            fig_width, fig_height = fig.get_size_inches() * fig.dpi
            logo_width, logo_height = logo_img.shape[1], logo_img.shape[0]
            x_pos = (fig_width - logo_width) / 2
            y_pos = (fig_height - logo_height) / 2
            fig.figimage(logo_img, xo=x_pos, yo=y_pos, alpha=0.5, zorder=0)
            
        ax.plot(df_historico['ANO'], df_historico['TotalValor'], marker='o', linestyle='-', color='b')
        ax.grid(True, which='both', linestyle='--', linewidth=0.5)
        ax.set_title(f'Evolução Anual de {tipo_consulta.capitalize()} - {local.title()}', fontsize=16, pad=20)
        ax.set_xlabel('Ano', fontsize=12)
        ylabel = f'Total de {tipo_consulta.capitalize()}'
        if tipo_consulta == 'cargas':
            ylabel += ' (kg)'
        ax.set_ylabel(ylabel, fontsize=12)
        formatter = mticker.FuncFormatter(lambda x, p: format(int(x), ','))
        ax.yaxis.set_major_formatter(formatter)
        ax.xaxis.set_major_locator(mticker.MaxNLocator(integer=True))
        
        # Define o fundo do eixo como transparente
        ax.patch.set_alpha(0.0)
        
        plt.tight_layout()
        buf = io.BytesIO()
        plt.savefig(buf, format='png', transparent=True)
        buf.seek(0)
        return buf
    except Exception as e:
        print(f"Erro ao gerar gráfico de histórico: {e}")
        return None 