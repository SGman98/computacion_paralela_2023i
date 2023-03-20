package co.edu.unal.paralela;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveTask;

/**
 * Clase que contiene los métodos para implementar la suma de los recíprocos de un arreglo usando
 * paralelismo.
 */
public final class ReciprocalArraySum {

  /** Constructor. */
  private ReciprocalArraySum() {}

  /**
   * Calcula secuencialmente la suma de valores recíprocos para un arreglo.
   *
   * @param input Arreglo de entrada
   * @return La suma de los recíprocos del arreglo de entrada
   */
  protected static double seqArraySum(final double[] input) {
    double sum = 0;

    // Calcula la suma de los recíprocos de los elementos del arreglo
    for (int i = 0; i < input.length; i++) {
      sum += 1 / input[i];
    }

    return sum;
  }

  /**
   * calcula el tamaño de cada trozo o sección, de acuerdo con el número de secciones para crear a
   * través de un número dado de elementos.
   *
   * @param nChunks El número de secciones (chunks) para crear
   * @param nElements El número de elementos para dividir
   * @return El tamaño por defecto de la sección (chunk)
   */
  private static int getChunkSize(final int nChunks, final int nElements) {
    // Función techo entera
    return (nElements + nChunks - 1) / nChunks;
  }

  /**
   * Este pedazo de clase puede ser completada para para implementar el cuerpo de cada tarea creada
   * para realizar la suma de los recíprocos del arreglo en paralelo.
   */
  private static class ReciprocalArraySumTask extends RecursiveTask<Double> {
    /** Iniciar el índice para el recorrido transversal hecho por esta tarea. */
    private final int startIndexInclusive;
    /** Concluir el índice para el recorrido transversal hecho por esta tarea. */
    private final int endIndexExclusive;
    /** Arreglo de entrada para la suma de recíprocos. */
    private final double[] input;

    /**
     * Constructor.
     *
     * @param setStartIndexInclusive establece el índice inicial para comenzar el recorrido
     *     trasversal.
     * @param setEndIndexExclusive establece el índice final para el recorrido trasversal.
     * @param setInput Valores de entrada
     */
    ReciprocalArraySumTask(
        final int setStartIndexInclusive, final int setEndIndexExclusive, final double[] setInput) {
      this.startIndexInclusive = setStartIndexInclusive;
      this.endIndexExclusive = setEndIndexExclusive;
      this.input = setInput;
    }

    @Override
    protected Double compute() {
      double sum = 0;
      for (int i = startIndexInclusive; i < endIndexExclusive; i++) {
        sum += 1 / input[i];
      }
      return sum;
    }
  }

  /**
   * Para hacer: Modificar este método para calcular la misma suma de recíprocos como le realizada
   * en seqArraySum, pero utilizando dos tareas ejecutándose en paralelo dentro del framework
   * ForkJoin de Java Se puede asumir que el largo del arreglo de entrada es igualmente divisible
   * por 2.
   *
   * @param input Arreglo de entrada
   * @return La suma de los recíprocos del arreglo de entrada
   */
  protected static double parArraySum(final double[] input) {
    assert input.length % 2 == 0;

    final int midPoint = input.length / 2;

    ReciprocalArraySumTask left = new ReciprocalArraySumTask(0, midPoint, input);
    ReciprocalArraySumTask right = new ReciprocalArraySumTask(midPoint, input.length, input);

    left.fork();
    right.fork();

    return left.join() + right.join();
  }

  /**
   * Para hacer: extender el trabajo hecho para implementar parArraySum que permita utilizar un
   * número establecido de tareas para calcular la suma del arreglo recíproco.
   * getChunkStartInclusive y getChunkEndExclusive pueden ser útiles para cacular el rango de
   * elementos índice que pertenecen a cada sección/trozo (chunk).
   *
   * @param input Arreglo de entrada
   * @param numTasks El número de tareas para crear
   * @return La suma de los recíprocos del arreglo de entrada
   */
  protected static double parManyTaskArraySum(final double[] input, final int numTasks) {
    List<ReciprocalArraySumTask> tasks = new ArrayList<>(numTasks);

    final int chunkSize = getChunkSize(numTasks, input.length);

    for (int i = 0; i < numTasks; i++) {
      tasks.add(new ReciprocalArraySumTask(i * chunkSize, (i + 1) * chunkSize, input));
    }

    return ForkJoinTask.invokeAll(tasks).parallelStream().mapToDouble(ForkJoinTask::join).sum();
  }
}
